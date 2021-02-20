package com.jkk.taskpublish;

import com.jkk.Task;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FairAndCanReUsingReBalanceRuleTest extends TestCase {
	@Override
	protected void setUp() throws Exception {

	}

	//0  0  0  0  0  0  0
	//1  1  1  0  0  0  0
	//2  1  1  2  0  0  0
	//2  1  1  2  3  0  0
	//4  1  1  2  3  0  0
	//4  5  1  2  3  0  0
	//4  5  1  2  3  6  0
	public void testNewTask() {
		int nodeNum = 7;
		Task[] task = new Task[7];
		for (int i=0; i< task.length; ++i) {
			task[i] = new Task();
			task[i].setJarId("jar-" + i);
			task[i].setProcessId(String.valueOf(i));
			task[i].setTopic("topic-" + i);
		}

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		System.out.println("初始: ");
		for (Task newTask : task) {
			Map<String, Task> map = reBalanceRule.newTask(newTask, processingTask, processedTask);

			for (String clientId : map.keySet()) {
				processingTask.put(clientId, map.get(clientId));
				if (! processedTask.get(clientId).offer(map.get(clientId))) {
					processedTask.get(clientId).poll();
					processedTask.get(clientId).offer(map.get(clientId));
				}
			}

			for (String clientId : processingTask.keySet()) {
				System.out.print(processingTask.get(clientId).getProcessId() + "  ");
			}
			System.out.println();
		}
	}

	//0  0  0  0  0  0  0
	//1  1  0  0  1  0  0
	//2  1  2  0  1  0  0
	//2  1  2  3  1  0  0
	//4  1  2  3  1  0  0
	//4  5  2  3  1  0  0
	//4  5  2  3  1  0  6
	public void testNewTask2() {
		int nodeNum = 7;
		Task[] task = new Task[7];
		for (int i=0; i< task.length; ++i) {
			task[i] = new Task();
			task[i].setJarId("jar-" + i);
			task[i].setProcessId(String.valueOf(i));
			task[i].setTopic("topic-" + i);
		}

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 4号节点执行过jar-1
		Task ranTask = new Task();
		ranTask.setProcessId("555");
		ranTask.setTopic("555");
		ranTask.setJarId("jar-1");
		processedTask.get("4").add(ranTask);

		// 6号节点执行过jar-6
		Task ranTask2 = new Task();
		ranTask2.setProcessId("556");
		ranTask2.setTopic("555");
		ranTask2.setJarId("jar-6");
		processedTask.get("6").add(ranTask2);

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		System.out.println("初始: ");
		for (Task newTask : task) {
			Map<String, Task> map = reBalanceRule.newTask(newTask, processingTask, processedTask);

			for (String clientId : map.keySet()) {
				processingTask.put(clientId, map.get(clientId));
				if (! processedTask.get(clientId).offer(map.get(clientId))) {
					processedTask.get(clientId).poll();
					processedTask.get(clientId).offer(map.get(clientId));
				}
			}

			for (String clientId : processingTask.keySet()) {
				System.out.print(processingTask.get(clientId).getProcessId() + "  ");
			}
			System.out.println();
		}
	}

	public void testBlockQueue() {
		BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(3);
		String[] off = {"2", "3", "4", "5", "6"};
		for (String i : off) {
			if (!blockingQueue.offer(i)) {
				blockingQueue.poll();
				blockingQueue.offer(i);
			}
		}
	}

	//初始:
	//0  0  0  0  0  0  0
	//1  1  0  0  1  0  0
	//任务1完成
	//0  0  0  0  0  0  0
	public void testFinishTask() {
		int nodeNum = 7;
		Task[] task = new Task[2];
		for (int i=0; i< task.length; ++i) {
			task[i] = new Task();
			task[i].setJarId("jar-" + i);
			task[i].setProcessId(String.valueOf(i));
			task[i].setTopic("topic-" + i);
		}

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 4号节点执行过jar-1
		Task ranTask = new Task();
		ranTask.setProcessId("555");
		ranTask.setTopic("555");
		ranTask.setJarId("jar-1");
		processedTask.get("4").add(ranTask);

		// 6号节点执行过jar-6
		Task ranTask2 = new Task();
		ranTask2.setProcessId("556");
		ranTask2.setTopic("555");
		ranTask2.setJarId("jar-6");
		processedTask.get("6").add(ranTask2);

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		System.out.println("初始: ");
		for (Task newTask : task) {
			Map<String, Task> map = reBalanceRule.newTask(newTask, processingTask, processedTask);

			for (String clientId : map.keySet()) {
				processingTask.put(clientId, map.get(clientId));
				if (! processedTask.get(clientId).offer(map.get(clientId))) {
					processedTask.get(clientId).poll();
					processedTask.get(clientId).offer(map.get(clientId));
				}
			}

			for (String clientId : processingTask.keySet()) {
				System.out.print(processingTask.get(clientId).getProcessId() + "  ");
			}
			System.out.println();
		}

		System.out.println("任务1完成");
		processingTask.put("0", new Task());
		processingTask.put("1", new Task());
		processingTask.put("4", new Task());

		Map<String, Task> map = reBalanceRule.finishTask(processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getProcessId() + "  ");
		}
		System.out.println();
	}

	//jar-3  jar-2  jar-4  jar-1  jar-1  jar-2  jar-3  jar-4
	public void testFinishTask2() {
		int nodeNum = 8;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("4", createTask(null, "jar-1", null));
		processingTask.put("5", createTask(null, "jar-2", null));
		processingTask.put("6", createTask(null, "jar-3", null));
		processingTask.put("7", createTask(null, "jar-4", null));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 0号节点执行过jar-1, jar-3
		processedTask.get("0").add(createTask(null, "jar-1", null));
		processedTask.get("0").add(createTask(null, "jar-3", null));
		// 1号节点执行过jar-1, jar-2
		processedTask.get("1").add(createTask(null, "jar-1", null));
		processedTask.get("1").add(createTask(null, "jar-2", null));
		// 2号节点执行过jar-3, jar-4
		processedTask.get("2").add(createTask(null, "jar-3", null));
		processedTask.get("2").add(createTask(null, "jar-4", null));
		// 3号节点执行过jar-1, jar-4
		processedTask.get("3").add(createTask(null, "jar-1", null));
		processedTask.get("3").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		Map<String, Task> map = reBalanceRule.finishTask(processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-2  jar-2  jar-1  jar-1  jar-2  jar-3  jar-3
	//jar-3  jar-2  jar-1  jar-1  jar-1  jar-2  jar-3  jar-3    1->2->1->3
	// 存在优化点, 将第三个 "jar-2" 变 "jar-3" 会更快
	public void testFinishTask3() {
		int nodeNum = 8;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("4", createTask(null, "jar-1", null));
		processingTask.put("5", createTask(null, "jar-2", null));
		processingTask.put("6", createTask(null, "jar-3", "topic-3"));
		processingTask.put("7", createTask(null, "jar-3", "topic-3"));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 0号节点执行过jar-1, jar-3
		processedTask.get("0").add(createTask(null, "jar-1", null));
		processedTask.get("0").add(createTask(null, "jar-3", null));
		// 1号节点执行过jar-1, jar-2
		processedTask.get("1").add(createTask(null, "jar-1", null));
		processedTask.get("1").add(createTask(null, "jar-2", null));
		// 2号节点执行过jar-3, jar-4
		processedTask.get("2").add(createTask(null, "jar-3", null));
		processedTask.get("2").add(createTask(null, "jar-4", null));
		// 3号节点执行过jar-1, jar-4
		processedTask.get("3").add(createTask(null, "jar-1", null));
		processedTask.get("3").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		Map<String, Task> map = reBalanceRule.finishTask(processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-4  jar-2  null
	public void testNodeCrash() {
		int nodeNum = 4;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("0", createTask(null, "jar-1", null));
		processingTask.put("1", createTask(null, "jar-2", "topic-2"));
		processingTask.put("2", createTask(null, "jar-2", "topic-2"));


		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		NodeTask node3 = new NodeTask();
		node3.setClientId("3");
		node3.setTask(createTask(null, "jar-4", null));

		Map<String, Task> map = reBalanceRule.nodeCrash(node3, processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-2  jar-4  null
	public void testNodeCrash2() {
		int nodeNum = 4;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("0", createTask(null, "jar-1", null));
		processingTask.put("1", createTask(null, "jar-2", "topic-2"));
		processingTask.put("2", createTask(null, "jar-2", "topic-2"));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 2号节点执行过jar-4
		processedTask.get("2").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		NodeTask node3 = new NodeTask();
		node3.setClientId("3");
		node3.setTask(createTask(null, "jar-4", null));

		Map<String, Task> map = reBalanceRule.nodeCrash(node3, processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-2  jar-3  jar-4  jar-5  jar-6  jar-7
	public void testNodeAdd() {
		int nodeNum = 6;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("0", createTask(null, "jar-1", null));
		processingTask.put("1", createTask(null, "jar-2", null));
		processingTask.put("2", createTask(null, "jar-3", null));
		processingTask.put("3", createTask(null, "jar-4", null));
		processingTask.put("4", createTask(null, "jar-5", null));
		processingTask.put("5", createTask(null, "jar-6", null));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 2号节点执行过jar-4
//		processedTask.get("2").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		Task task6 = createTask(null, "jar-7", null);
		NodeTask node6 = new NodeTask();
		node6.setClientId("6");
		node6.setTask(task6);
		processingTask.put("6", task6);

		Map<String, Task> map = reBalanceRule.nodeAdd(node6, processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-1  jar-3  jar-4  jar-5  jar-6  jar-3
	public void testNodeAdd2() {
		int nodeNum = 6;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("0", createTask(null, "jar-1", "topic-1"));
		processingTask.put("1", createTask(null, "jar-1", "topic-1"));
		processingTask.put("2", createTask(null, "jar-3", null));
		processingTask.put("3", createTask(null, "jar-4", null));
		processingTask.put("4", createTask(null, "jar-5", null));
		processingTask.put("5", createTask(null, "jar-6", null));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 2号节点执行过jar-4
//		processedTask.get("2").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		Task task6 = createTask(null, "jar-7", null);
		NodeTask node6 = new NodeTask();
		node6.setClientId("6");
		node6.setTask(new Task());
		processingTask.put("6", new Task());
		processedTask.put("6", new ArrayBlockingQueue<>(20));
		Map<String, Task> map = reBalanceRule.nodeAdd(node6, processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	//jar-1  jar-1  jar-3  jar-4  jar-5  jar-6  jar-4
	public void testNodeAdd3() {
		int nodeNum = 6;

		Map<String, Task> processingTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processingTask.put("" + i, new Task());
		}
		processingTask.put("0", createTask(null, "jar-1", "topic-1"));
		processingTask.put("1", createTask(null, "jar-1", "topic-1"));
		processingTask.put("2", createTask(null, "jar-3", null));
		processingTask.put("3", createTask(null, "jar-4", "topic-4"));
		processingTask.put("4", createTask(null, "jar-5", null));
		processingTask.put("5", createTask(null, "jar-6", null));

		Map<String, BlockingQueue<Task>> processedTask = new HashMap<>();
		for (int i=0; i<nodeNum; ++i) {
			processedTask.put("" + i, new ArrayBlockingQueue<>(20));
		}
		// 2号节点执行过jar-4
		processedTask.get("2").add(createTask(null, "jar-4", null));

		ReBalanceRule reBalanceRule = new FairAndCanReUsingReBalanceRule();

		Task task6 = createTask(null, "jar-7", null);
		NodeTask node6 = new NodeTask();
		node6.setClientId("6");
		node6.setTask(new Task());
		processingTask.put("6", new Task());
		processedTask.put("6", new ArrayBlockingQueue<>(20));
		processedTask.get("6").add(createTask(null, "jar-4", "topic-4"));

		Map<String, Task> map = reBalanceRule.nodeAdd(node6, processingTask, processedTask);
		for (String clientId : map.keySet()) {
			processingTask.put(clientId, map.get(clientId));
			if (! processedTask.get(clientId).offer(map.get(clientId))) {
				processedTask.get(clientId).poll();
				processedTask.get(clientId).offer(map.get(clientId));
			}
		}
		for (String clientId : processingTask.keySet()) {
			System.out.print(processingTask.get(clientId).getJarId() + "  ");
		}
		System.out.println();
	}

	public static Task createTask(String processId, String jarId, String topicId) {
		Task task = new Task();

		task.setTopic(topicId == null? UUID.randomUUID().toString(): topicId);
		task.setProcessId(processId == null? UUID.randomUUID().toString(): processId);
		task.setJarId(jarId == null? UUID.randomUUID().toString(): jarId);
		return task;
	}

}