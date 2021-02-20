package com.jkk.taskresolve;

import com.jkk.Task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
	private static final String zk = "hadoop000:2181";
	private static final String namespace = "test1/available";
	private static final CountDownLatch countDownLatch = new CountDownLatch(1);

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("输入一个ID");
		}

		AtomicInteger i = new AtomicInteger(0);
		TaskResolver taskResolver = new TaskResolver(zk, namespace, args[0], new TaskHandle() {
			@Override
			public void doTask(Task task) {
				System.out.println("正在执行的任务:" + task);
				i.getAndIncrement();
			}
		});

		new Thread(() -> {
			while (true) {
				if (i.get() == 2) {
					taskResolver.finish();
					break;
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}).start();


		countDownLatch.await();
	}

}
