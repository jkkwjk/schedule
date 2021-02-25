package com.jkk.taskresolve;

import com.jkk.Task;

import java.util.Scanner;

public class Main {
	private static final String zk = "hadoop000:2181";
	private static final String namespace = "test1/available";

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("输入一个ID");
		}

		TaskResolver taskResolver = new TaskResolver(zk, namespace, args[0], new TaskHandle() {
			@Override
			public void doTask(Task task) {
				System.out.println("正在执行的任务:" + task);
			}
		});

		Scanner scanner = new Scanner(System.in);

		while (scanner.hasNext()) {
			scanner.next();
			taskResolver.finish();
		}
	}

}
