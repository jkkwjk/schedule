package com.jkk.taskpublish;

import com.alibaba.fastjson.JSON;
import com.jkk.Task;
import lombok.Data;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Queue;

@Data
public class TaskPublisherTest {
	private static final String zk = "hadoop000:2181";

	public static void main(String[] args) throws Exception {

//		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
//		CuratorFramework client =
//				CuratorFrameworkFactory.builder()
//						.connectString(zk)
//						.namespace("test1/available")
//						.retryPolicy(retryPolicy)
//						.sessionTimeoutMs(3000)
//						.connectionTimeoutMs(3000).build();
//
//		client.start();
//
//
//		Task task = new Task();
//		task.setTopic("topic id");
//		task.setJarId("jar id");
//		task.setProcessId("process id2");
//
//		client.setData().forPath("/1", JSON.toJSONString(task).getBytes());
	}
}
