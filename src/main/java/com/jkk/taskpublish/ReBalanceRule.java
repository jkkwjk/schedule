package com.jkk.taskpublish;

import com.jkk.Task;
import com.jkk.taskpublish.entity.NodeTask;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public interface ReBalanceRule {

	/**
	 *
	 * @param newTask 新的任务
	 * @param processingTask 节点正在执行的任务
	 * @param processedTask 节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	Map<String, Task> newTask(Task newTask,
		                      Map<String, Task> processingTask,
		                      Map<String, BlockingQueue<Task>> processedTask);


	/**
	 * 有节点任务完成 ({@link Task#getProcessId()==null})后
	 * 等待其他节点的{@link Task#equals(Object 当前finish的任务)}都完成后调用这个函数
	 * @param processingTask 节点正在执行的任务
	 * @param processedTask 节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	Map<String, Task> finishTask(Map<String, Task> processingTask,
	                             Map<String, BlockingQueue<Task>> processedTask);


	/**
	 * 节点挂了之后执行的动作
	 * @param crashNode 下线的节点
	 * @param processingTask 所有节点正在执行的任务
	 * @param processedTask 所有节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	Map<String, Task> nodeCrash(NodeTask crashNode,
	                            Map<String, Task> processingTask,
	                            Map<String, BlockingQueue<Task>> processedTask);


	/**
	 * 节点加入之后执行的动作
	 * @param addNode 节点 (新上线节点可能会有旧任务运行)
	 * @param processingTask 所有节点正在执行的任务
	 * @param processedTask 所有节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	Map<String, Task> nodeAdd(NodeTask addNode,
	                          Map<String, Task> processingTask,
	                          Map<String, BlockingQueue<Task>> processedTask);

}
