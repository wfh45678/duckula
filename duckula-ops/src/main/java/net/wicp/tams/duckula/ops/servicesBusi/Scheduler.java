package net.wicp.tams.duckula.ops.servicesBusi;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.tapestry5.ioc.annotations.Inject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.ZkPath;

@Component
@Slf4j
public class Scheduler {

	@Inject
	@Autowired
	private IDuckulaAssit duckulaAssit;

	@PostConstruct
	private void init() {
		runUnStart();
		log.info("int exec sucess");
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
		service.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				runUnStart();
			}
		}, 10, 10, TimeUnit.MINUTES);
	}

	/***
	 * 启动异常后失败的文件
	 */
	public void runUnStart() {
		List<String> taskIds = ZkUtil.findSubNodes(ZkPath.tasks);
		for (String taskId : taskIds) {
			duckulaAssit.reStartTask(CommandType.task, taskId);
		}
		List<String> consumers = ZkUtil.findSubNodes(ZkPath.consumers);
		for (String consumerId : consumers) {
			duckulaAssit.reStartTask(CommandType.consumer, consumerId);
		}
	}
}
