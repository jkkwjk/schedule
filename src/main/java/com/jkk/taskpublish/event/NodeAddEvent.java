package com.jkk.taskpublish.event;

import com.alibaba.fastjson.JSON;
import com.jkk.Task;
import com.jkk.taskpublish.ReBalanceRule;
import com.jkk.taskpublish.entity.NodeTask;
import com.jkk.taskpublish.exception.OutOfResourcesException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class NodeAddEvent implements EventStrategy {
	private final ReBalanceRule reBalanceRule;
	private final Integer queueNum;

	public NodeAddEvent(ReBalanceRule reBalanceRule, Integer queueNum) {
		this.reBalanceRule = reBalanceRule;
		this.queueNum = queueNum;
	}
	@Override
	public Map<String, Task> exec(CuratorFramework curatorFramework,
	                              Boolean isInit,
	                              ChildData data,
	                              Map<String, Task> processingTask,
	                              Map<String, BlockingQueue<Task>> processedTask,
	                              Map<String, BlockingQueue<Task>> processedCache) {

		Task ranTask = JSON.parseObject(new String(data.getData()), Task.class);
		String newClientId = data.getPath().substring(1);

		processingTask.put(newClientId, ranTask);
		processedTask.put(newClientId, processedCache.getOrDefault(newClientId, new ArrayBlockingQueue<>(queueNum)));
		processedCache.remove(newClientId);

		NodeTask nodeTask = new NodeTask();
		nodeTask.setClientId(newClientId);
		nodeTask.setTask(ranTask);

		Map<String, Task> modifyNodes = new HashMap<>();
		try {
			modifyNodes = reBalanceRule.nodeAdd(nodeTask, processingTask, processedTask);
			return modifyNodes;
		}catch (OutOfResourcesException e) {
			e.printStackTrace();
			return modifyNodes;
		}
	}
}
