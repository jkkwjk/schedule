package com.jkk.taskpublish.event;

import com.alibaba.fastjson.JSON;
import com.jkk.Task;
import com.jkk.taskpublish.ReBalanceRule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class NodeUpdateEvent implements EventStrategy {

	private final ReBalanceRule reBalanceRule;

	public NodeUpdateEvent(ReBalanceRule reBalanceRule) {
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
		String newClientId = data.getPath().substring(1);

		if (ranTask.getProcessId() != null) { // 节点更新时存在正在执行的任务, 来自任务发布者的update
			return new HashMap<>();
		}

		processingTask.put(newClientId, ranTask);

		Map<String, Task> modifyNodes = reBalanceRule.finishTask(processingTask, processedTask);

		// 过滤再次分配上一个任务的情况
		// FIXME: 2021/2/25 最好在finishTask处理, 分配另一个任务而不是不分配任务, 或者等待也能解决???
		return modifyNodes.entrySet().stream().filter(o -> {
			Task[] tasks = (Task[]) processedTask.get(newClientId).toArray();
			return ! modifyNodes.get(o.getKey()).equals(tasks[tasks.length - 1]);
		}).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}
}
