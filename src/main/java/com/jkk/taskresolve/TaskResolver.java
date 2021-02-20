package com.jkk.taskresolve;

import com.alibaba.fastjson.JSON;
import com.jkk.Task;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TaskResolver {
	private Task nowTask = new Task();
	private CuratorFramework client;
	private final String id;

	public TaskResolver(String zkConnectString, String namespace, String id, TaskHandle taskHandle) {
		this.id = id;
		this.run(zkConnectString, namespace, taskHandle);
	}

	private void run(String zkConnectString, String namespace, TaskHandle taskHandle) {
		AtomicBoolean isFirstStart = new AtomicBoolean(true);
		log.info("程序ID: {}", id);

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client =
				CuratorFrameworkFactory.builder()
				.connectString(zkConnectString)
				.namespace(namespace)
				.retryPolicy(retryPolicy)
				.sessionTimeoutMs(3000)
				.connectionTimeoutMs(3000).build();

		NodeCache nodeCache = new NodeCache(client, "/" + id, false);
		nodeCache.getListenable().addListener(() -> {
			String nowData = new String(nodeCache.getCurrentData().getData());
			if (! nowData.equals(JSON.toJSONString(nowTask)) && ! nowData.equals("{}")) {
				nowTask = JSON.parseObject(nowData, Task.class);
				taskHandle.doTask(nowTask);
				log.info("新任务发布: {}", nowTask);
			}
		});

		client.getConnectionStateListenable().addListener((curatorFramework, connectionState) -> {
			if (connectionState.isConnected()) {
				log.info("连接成功");
				// 上线
				try {
					// 保证前一次连接的session过期
					while (client.getChildren().forPath("/").stream().anyMatch(s -> s.equals(id))) {
						log.warn("节点创建失败");
						Thread.sleep(2000);
					}

					client.create()
							.withMode(CreateMode.EPHEMERAL)
							.forPath("/" + id, JSON.toJSONString(nowTask).getBytes());
					log.info("节点创建成功");

					if (isFirstStart.get()) {
						nodeCache.start(true);
						isFirstStart.set(false);
					}

				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}else {
				log.warn("断开连接");
			}
		});
		client.start();
	}

	@SneakyThrows
	public void finish() {
		client.setData().forPath("/" + id, "{}".getBytes());
		log.info("任务完成");
	}
}
