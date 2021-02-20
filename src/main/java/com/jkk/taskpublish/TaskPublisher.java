package com.jkk.taskpublish;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.Scanner;

public class TaskPublisher {
	private static final String zk = "hadoop000:2181";

	public static void main(String[] args) throws Exception {
		boolean isInit = false;
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client =
				CuratorFrameworkFactory.builder()
						.connectString(zk)
						.namespace("test1/available")
						.retryPolicy(retryPolicy)
						.sessionTimeoutMs(3000)
						.connectionTimeoutMs(3000).build();
		client.start();

		PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/", true);
		pathChildrenCache.getListenable().addListener((curatorFramework, pathChildrenCacheEvent) -> {
			if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
				System.out.println("节点加入" + pathChildrenCacheEvent.getData());
			}else if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
				// 节点删除之后任务不会延续
				System.out.println("节点删除" + pathChildrenCacheEvent.getData());
			}else if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
				System.out.println("节点更新" + pathChildrenCacheEvent.getData());
			}else if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.INITIALIZED) {
				System.out.println("初始化完成");
			}else {
				System.out.println(pathChildrenCacheEvent.getType());
			}

		});
		pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);


		Scanner sc = new Scanner(System.in);
		if (sc.hasNext()) {
			Stat stat = new Stat();
			String s = sc.next();


//			List<String> strings = client.getChildren().forPath("/");
		}

	}
}
