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
import java.util.concurrent.BlockingQueue;

public class NodeRemoveEvent implements EventStrategy {
	private final ReBalanceRule reBalanceRule;

	public NodeRemoveEvent(ReBalanceRule reBalanceRule) {
		this.reBalanceRule = reBalanceRule;
	}

	@Override
	public Map<String, Task> exec(CuratorFramework curatorFramework,
	                              Boolean isInit,
	                              ChildData data,
	                              Map<String, Task> processingTask,
	                              Map<String, BlockingQueue<Task>> processedTask,
	                              Map<String, BlockingQueue<Task>> processedCache) {

		Task ranTask = JSON.parseObject(new String(data.getData()), Task.class);
		String crashClientId = data.getPath().substring(1);
		NodeTask nodeTask = new NodeTask();
		nodeTask.setTask(ranTask);
		nodeTask.setClientId(crashClientId);

		processingTask.remove(crashClientId);
		processedCache.put(crashClientId, processedTask.get(crashClientId));
		processedTask.remove(crashClientId);

		Map<String, Task> modifyNodes = new HashMap<>();
		try {
			modifyNodes = reBalanceRule.nodeCrash(nodeTask, processingTask, processedTask);
			return modifyNodes;
		}catch (OutOfResourcesException e) {
			e.printStackTrace();
			return modifyNodes;
		}
	}
}
