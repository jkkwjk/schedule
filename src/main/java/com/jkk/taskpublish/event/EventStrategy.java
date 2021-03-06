package com.jkk.taskpublish.event;

import com.jkk.Task;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public interface EventStrategy {
	Map<String, Task> exec(CuratorFramework curatorFramework,
			  Boolean isInit,
	          ChildData data,
	          Map<String, Task> processingTask,
	          Map<String, BlockingQueue<Task>> processedTask,
	          Map<String, BlockingQueue<Task>> processedCache) throws Exception;

	default void update(CuratorFramework curatorFramework,
	                    Map<String, Task> processingTask,
	                    Map<String, BlockingQueue<Task>> processedTask,
	                    Map<String, Task> modifyNodes) throws Exception {

	}
}
