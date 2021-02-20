package com.jkk.taskpublish;

import com.jkk.Task;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * 每个任务的权重是一样的, 且优先选择之前执行过该任务的节点执行
 * 优先选择执行过的任务执行, 且保证一个任务一定有一个节点执行
 * 保证均衡的基础上提升效率
 */
public class FairAndCanReUsingReBalanceRule implements ReBalanceRule {

	/**
	 * 保证每个任务占据集群内比重一致
	 * 此前提下优先把任务分配给 节点之前执行过的任务 {@link Task#canReUsingTask(Task)} 为True的节点
	 * @param newTask 新的任务
	 * @param processingTask 节点正在执行的任务
	 * @param processedTask 节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	@Override
	public Map<String, Task> newTask(Task newTask, Map<String, Task> processingTask, Map<String, BlockingQueue<Task>> processedTask) {
		Map<String, Task> result = new HashMap<>();

		// 分配所有空闲的节点
		List<String> freeNodes = filterFreeNode(processingTask);
		result.putAll(freeNodes.stream().collect(Collectors.toMap(item -> item, item -> newTask)));

		// 按照均衡策略 应当分配 总结点数/任务总数 给他
		int nodeNum = processingTask.size();
		int taskNum = new HashSet<>(processingTask.values()).size() + 1;
		if (taskNum > nodeNum) {
			throw new OutOfResourcesException(newTask);
		}

		int needDistributeNum = nodeNum / taskNum - result.size(); // 还需要分配的节点数
		if (needDistributeNum > 0) {
			List<String> processedNodes = filterProcessed(newTask, processedTask); // 运行过该任务的节点

			PriorityQueue<TaskNodes> priorityQueue = new PriorityQueue<>((o1, o2) -> o2.getNodes().size() - o1.getNodes().size()); // 从运行任务节点多的开始
			priorityQueue.addAll(processingTask2TaskNodeList(processingTask));

			while (needDistributeNum-- != 0) {
				TaskNodes taskNodes = priorityQueue.poll();

				Optional<String> clientId = processedNodes.stream().filter(taskNodes::existNodes).findFirst();// 优先选择之前执行过的
				String removeClientId = clientId.orElseGet(() -> taskNodes.getNodes().get(0));

				taskNodes.removeNodes(removeClientId);
				result.put(removeClientId, newTask);

				priorityQueue.add(taskNodes);
			}
		}

		return result;
	}

	/**
	 * 得到任务在哪些freeNodes中执行过
	 * @param task
	 * @param freeNodes
	 * @param processedTask
	 * @return
	 */
	private TaskNodes getFreeTaskNodes(Task task, Set<String> freeNodes, Map<String, BlockingQueue<Task>> processedTask) {
		TaskNodes newTaskNodes = new TaskNodes(task);
		filterProcessed(task, processedTask).stream().filter(freeNodes::contains).forEach(newTaskNodes::addNodes);
		return newTaskNodes;
	}
	/**
	 * 将运行资源少的任务分配给空闲节点
	 * @param processingTask 节点正在执行的任务
	 * @param processedTask 节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	@Override
	public Map<String, Task> finishTask(Map<String, Task> processingTask, Map<String, BlockingQueue<Task>> processedTask) {
		Map<String, Task> result = new HashMap<>();

		Set<String> freeNodes = new HashSet<>(filterFreeNode(processingTask));

		Map<Task, TaskNodes> newTaskNodesCache = new HashMap<>(); // 任务执行过的节点 存在空闲节点中的
		List<TaskNodes> taskNodesList = processingTask2TaskNodeList(processingTask);
		for (TaskNodes taskNodes : taskNodesList) {
			newTaskNodesCache.put(taskNodes.getTask(), getFreeTaskNodes(taskNodes.getTask(), freeNodes, processedTask));
		}

		PriorityQueue<TaskNodes> priorityQueue = new PriorityQueue<>((o1, o2) -> {
			int cmp = o1.getNodes().size() - o2.getNodes().size();
			if (cmp != 0) {
				return cmp;
			}else {
				// 正在执行节点个数相等下 优先考虑执行过该任务的空闲节点中多的
				int inFreeNodes1 = newTaskNodesCache.get(o1.getTask()).getNodes().size();
				int inFreeNodes2 = newTaskNodesCache.get(o2.getTask()).getNodes().size();
				return inFreeNodes2 - inFreeNodes1;
			}
		}); // 从运行任务节少的开始
		priorityQueue.addAll(taskNodesList);

		List<TaskNodes> newTaskNodesList = new ArrayList<>(); // 需要分配的任务 对应的执行过的 在freeNodes中的节点 可能存在多个相同的任务
		for (int i = 0; i < freeNodes.size(); ++i) {
			TaskNodes taskNodes = priorityQueue.poll();
			taskNodes.addNodes("ohhhhhhhh"); // 添加的虚拟节点, 方便改变优先队列顺序, 它的值对结果没有任何影响

			newTaskNodesList.add(newTaskNodesCache.get(taskNodes.getTask()));
			priorityQueue.add(taskNodes);
		}

		newTaskNodesList.sort(Comparator.comparingInt(o -> o.getNodes().size())); // 从少到多分配
		boolean[] isNewTaskNodesDistribute = new boolean[newTaskNodesList.size()];
		for (int i = 0; i < newTaskNodesList.size(); ++i) {
			for (String clientId : newTaskNodesList.get(i).getNodes()) {
				if (freeNodes.contains(clientId)) {
					freeNodes.remove(clientId);
					result.put(clientId, newTaskNodesList.get(i).getTask());
					isNewTaskNodesDistribute[i] = true;
					break;
				}
			}
		}

		// 分配不存在可重用任务的节点
		for (int i = 0; i < isNewTaskNodesDistribute.length; ++i) {
			if (! isNewTaskNodesDistribute[i]) {
				// get() 一定会存在, 不然前面的白写了
				result.put(freeNodes.stream().findFirst().get(), newTaskNodesList.get(i).getTask());
			}
		}

		return result;
	}

	/**
	 * 可能因为网络波动造成节点下线, 节点可能继续执行任务, 也可能在短时间内恢复
	 *
	 * 策略:
	 * 如果 下线节点的任务只在该节点上运行 则占用另外节点执行该任务
	 * 否则 不改变任何节点
	 * @param crashNode 下线的节点
	 * @param processingTask 所有节点正在执行的任务
	 * @param processedTask 所有节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	// TODO: 2021/2/18 需要先 在processingTask删除这个节点  processedTask放在备用map中
	@Override
	public Map<String, Task> nodeCrash(NodeTask crashNode, Map<String, Task> processingTask, Map<String, BlockingQueue<Task>> processedTask) {
		Map<String, Task> result = new HashMap<>();

		List<TaskNodes> taskNodesList = processingTask2TaskNodeList(processingTask);

		Task runningTask = crashNode.getTask();
		if (runningTask.getProcessId() != null) {
			List<String> ranNodes = filterProcessed(runningTask, processedTask);

			// 没有节点执行这个任务了
			if (! taskNodesList.contains(new TaskNodes(runningTask))) {
				TaskNodes maxRunTaskNodes = taskNodesList.stream().max(Comparator.comparingInt(o -> o.getNodes().size())).get();
				if (maxRunTaskNodes.getNodes().size() <= 1) {
					throw new OutOfResourcesException(runningTask);
				}

				String removeClientId = maxRunTaskNodes.getNodes().stream().filter(ranNodes::contains).findFirst()
						.orElseGet(() -> maxRunTaskNodes.getNodes().get(0));

				maxRunTaskNodes.removeNodes(removeClientId);
				result.put(removeClientId, runningTask);
			}
		}

		return result;
	}

	/**
	 * 上线时正在执行任务的 继续执行老任务
	 * 否则 下发获得资源最少的任务
	 * @param addNode 节点 (新上线节点可能会有旧任务运行)
	 * @param processingTask 所有节点正在执行的任务
	 * @param processedTask 所有节点之前执行过的任务
	 * @return 需要更新任务的节点
	 */
	@Override
	public Map<String, Task> nodeAdd(NodeTask addNode, Map<String, Task> processingTask, Map<String, BlockingQueue<Task>> processedTask) {
		Map<String, Task> result = new HashMap<>();

		Task task = addNode.getTask();
		if (task.getProcessId() == null) {
			return finishTask(processingTask, processedTask);
		}

		return result;
	}

	/**
	 *
	 * @param task
	 * @param processedTask
	 * @return 可以重用该任务的节点
	 */
	private List<String> filterProcessed(Task task, Map<String, BlockingQueue<Task>> processedTask) {
		List<String> result = new LinkedList<>();

		for (String clientId : processedTask.keySet()) {
			boolean isProcessed = processedTask.get(clientId).stream().anyMatch(item -> item.canReUsingTask(task));
			if (isProcessed) {
				result.add(clientId);
			}
		}

		return result;
	}

	/**
	 *
	 * @param processingTask
	 * @return 空闲的节点
	 */
	private List<String> filterFreeNode(Map<String, Task> processingTask) {
		List<String> freeNodes = new LinkedList<>();
		for (String clientId : processingTask.keySet()) {
			if (processingTask.get(clientId).getProcessId() == null) {
				freeNodes.add(clientId);
			}
		}

		return freeNodes;
	}

	/**
	 *
	 * @param processingTask
	 * @return 某个任务对应一堆节点的列表 会过滤掉空闲的节点
	 */
	private List<TaskNodes> processingTask2TaskNodeList(Map<String, Task> processingTask) {
		List<TaskNodes> taskNodesList = new ArrayList<>();
		for (String clientId : processingTask.keySet()) {
			if (processingTask.get(clientId).getProcessId() != null) {
				TaskNodes taskNodes = new TaskNodes(processingTask.get(clientId), clientId);

				int i = taskNodesList.indexOf(taskNodes);
				if (i == -1) {
					taskNodesList.add(taskNodes);
				}else {
					taskNodesList.get(i).addNodes(clientId);
				}
			}
		}

		return taskNodesList;
	}
}
