package net.wicp.tams.duckula.task.test.zk;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import net.wicp.tams.common.Result;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.SenderEnum;
import net.wicp.tams.duckula.common.constant.ZkPath;

public class ZkTest {
	private CuratorFramework retframework;

	@Test
	public void newClient() {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		retframework = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
		retframework.start();
	}

	@Test
	public void updateTask() {
		ZkClient.getInst().createMultilevelNode(ZkPath.tasks.getRoot());
		Task task = new Task();
		task.setClientId(569);
		task.setDbinst("localhost");
		task.setDefaultDb("demo");
		task.setId("L-demo-city");
		task.setIp("localhost");
		task.setRds(YesOrNo.yes);
		task.setPort(3306);
		task.setPwd("111111");
		task.setRules("demo,*,aaaa");
		task.setSenderEnum(SenderEnum.no);
		task.setThreadNum(1);
		task.setUser("root");
		Result result = ZkClient.getInst().createOrUpdateNodeForJson(ZkPath.tasks.getPath("L-demo-city"), task);
		System.out.println(result);
	}

	@Test
	public void LockTask() throws Exception {
		InterProcessMutex lock = new InterProcessMutex(ZkClient.getInst().getCurator(), "/duckula/tasks/demo_user");
		boolean hasLock = lock.acquire(60, TimeUnit.SECONDS);
		if (hasLock) {
			System.out.println("ok");
		} else {
			System.out.println("error");
		}
		Collection<String> lockNodes = lock.getParticipantNodes();
		for (String ele : lockNodes) {
			System.out.println(ele);
			String val = ZkClient.getInst().getZkDataStr(ele);
			System.out.println(val);
		}
		lock.release();
	}

}
