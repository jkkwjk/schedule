package com.jkk.taskpublish;

import com.jkk.Task;

import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

public class Main {
	public static void main(String[] args) throws Exception {
		TaskPublisher taskPublisher = new TaskPublisher("hadoop000:2181",
				"test1/available",
				new FairAndCanReUsingReBalanceRule(),
				20);

		Scanner sc = new Scanner(System.in);
		while (sc.hasNext()) {
			String s = sc.next();
			Task task = new Task();
			task.setJarId(UUID.randomUUID().toString());
			task.setProcessId(UUID.randomUUID().toString());
			task.setTopic(UUID.randomUUID().toString());
			taskPublisher.newTask(task);
		}
	}
}
