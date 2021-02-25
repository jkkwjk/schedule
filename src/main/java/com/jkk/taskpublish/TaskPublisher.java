package com.jkk.taskpublish;

import com.alibaba.fastjson.JSON;
import com.jkk.Task;
import com.jkk.taskpublish.event.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class TaskPublisher {
	private final String zkConnectString;
	private final String namespace;
	private final Integer queueNum;
	private final ReBalanceRule reBalanceRule;
	private CuratorFramework client;

	private final Map<String, Task> processingTask;
	private final Map<String, BlockingQueue<Task>> processedTask;
	private final Map<String, BlockingQueue<Task>> processedCache;

	public TaskPublisher(String zkConnectString, String namespace, ReBalanceRule reBalanceRule, Integer queueNum) throws Exception {
		this.zkConnectString = zkConnectString;
		this.namespace = namespace;
		this.reBalanceRule = reBalanceRule;
		this.queueNum = queueNum;

		this.processingTask = new HashMap<>();
		this.processedTask = new HashMap<>();
		this.processedCache = new HashMap<>();
		this.start();
	}

	private void start() throws Exception {
		AtomicReference<Boolean> isInit = new AtomicReference<>(false);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.builder()
						.connectString(zkConnectString)
						.namespace(namespace)
						.retryPolicy(retryPolicy)
						.sessionTimeoutMs(3000)
						.connectionTimeoutMs(3000).build();
		client.start();

		Map<PathChildrenCacheEvent.Type, EventStrategy> eventStrategy = new HashMap<>();
		eventStrategy.put(PathChildrenCacheEvent.Type.CHILD_ADDED, new NodeAddEvent(reBalanceRule, queueNum));
		eventStrategy.put(PathChildrenCacheEvent.Type.CHILD_REMOVED, new NodeRemoveEvent(reBalanceRule));
		eventStrategy.put(PathChildrenCacheEvent.Type.CHILD_UPDATED, new NodeUpdateEvent(reBalanceRule));
		eventStrategy.put(PathChildrenCacheEvent.Type.INITIALIZED, new InitializedEvent());
		DefaultEvent defaultEvent = new DefaultEvent();

		PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/", true);
		pathChildrenCache.getListenable().addListener((curatorFramework, pathChildrenCacheEvent) -> {
			System.out.println(pathChildrenCacheEvent.getType() + ": " + pathChildrenCacheEvent.getData().toString());
			EventStrategy strategy = eventStrategy.getOrDefault(pathChildrenCacheEvent.getType(), defaultEvent);
			Map<String, Task> modifyNodes = strategy.exec(curatorFramework, isInit.get(), pathChildrenCacheEvent.getData(), processingTask, processedTask, processedCache);
			update(curatorFramework, processingTask, processedTask, modifyNodes);
		});
		pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

	}

	private void update(CuratorFramework curatorFramework,
	                          Map<String, Task> processingTask,
	                          Map<String, BlockingQueue<Task>> processedTask,
	                          Map<String, Task> modifyNodes) throws Exception {
		for (String clientId : modifyNodes.keySet()) {
			System.out.println("修改" + clientId + ":" + modifyNodes.get(clientId));
			curatorFramework.setData()
					.forPath("/" + clientId, JSON.toJSONString(modifyNodes.get(clientId)).getBytes());

			processingTask.put(clientId, modifyNodes.get(clientId));
			if (! processedTask.get(clientId).offer(modifyNodes.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(modifyNodes.get(clientId));
			}
		}
	}

	public void newTask(Task task) throws Exception {
		Map<String, Task> res = reBalanceRule.newTask(task, processingTask, processedTask);
		update(client, processingTask, processedTask, res);
	}
}
