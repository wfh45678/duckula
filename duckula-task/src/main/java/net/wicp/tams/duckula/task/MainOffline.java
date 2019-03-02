package net.wicp.tams.duckula.task;

import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.beans.Host;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.metrics.utility.TsLogger;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Count;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.task.disruptor.DisruptorProducer;
import net.wicp.tams.duckula.task.parser.ParseLogOffline;

@Slf4j
public class MainOffline {

	public static void main(String[] args) {
		if (args == null || args.length < 1) {
			log.error("必须传offlineId");
			return;
		}
		String offlineId = args[0];
		// -----------------配置信息，主要是zookeeper的配置信息
		CommandType.task.setCommonProps();
		/*
		 * InterProcessMutex lock = ZkUtil.lockTaskOfflinePath(offlineId); try { if
		 * (!lock.acquire(60, TimeUnit.SECONDS)) { List<String> ips =
		 * ZkClient.getInst().lockValueList(lock); log.error("已有服务[{}]在运行中,无法获得锁.",
		 * CollectionUtil.listJoin(ips, ",")); LoggerUtil.exit(JvmStatus.s9); } } catch
		 * (Exception e1) { log.error("获取锁异常", e1); LoggerUtil.exit(JvmStatus.s9); }
		 */
		// metrix
		System.setProperty(TsLogger.ENV_FILE_NAME, "tams_" + offlineId);
		System.setProperty(TsLogger.ENV_FILE_ROOT, String.format("%s/logs/metrics", System.getenv("DUCKULA_DATA")));

		Main.metric = new DuckulaGroup(offlineId);
		log.info("----------------------执行的服务器信息-------------------------------------");
		try {
			InetAddress address = InetAddress.getLocalHost();
			String hostIp = address.getHostAddress();
			Host host = Host.builder().hostIp(hostIp).port(StringUtil.buildPort(offlineId)).build();//
			Main.context.setHost(host);
		} catch (Exception e) {
			log.error("主机信息错误", e);
			LoggerUtil.exit(JvmStatus.s15);
		}
		///////////// 计数器
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
		service.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				updatePosAndCount(offlineId, false);
			}
		}, 10, 1, TimeUnit.SECONDS);

		TaskOffline buidlTaskOffline = ZkUtil.buidlTaskOffline(offlineId);
		buidlTaskOffline.setRunStartTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		buidlTaskOffline.setRunEndTime(" ");
		ZkClient.getInst().createOrUpdateNode(ZkPath.tasksofflines.getPath(buidlTaskOffline.getId()),
				JSONObject.toJSONString(buidlTaskOffline));
		Main.context.setTask(buidlTaskOffline.getTaskOnline());// DisruptorProducer需要用到 TaskOnline
		Main.context.setParsePos(new Pos());// 离线不需要记录Pos，不设置会报错TODO
		ParseLogOffline parseLogOffline = new ParseLogOffline(buidlTaskOffline, new DisruptorProducer(true));
		parseLogOffline.read();
		updatePosAndCount(offlineId, true);
		parseLogOffline.close();		
	}

	private static void updatePosAndCount(String offlineId, boolean isover) {
		Count.CountBuilder build = Count.builder().insertNum(Main.metric.meter_sender_event_add.getCount());
		build.updateNum(Main.metric.meter_sender_event_update.getCount());
		build.deleteNum(Main.metric.meter_sender_event_del.getCount());
		build.allPack(Main.metric.meter_parser_pack_all.getCount());
		build.parserPack(Main.metric.meter_parser_pack_row.getCount());
		build.parserEvent(Main.metric.meter_parser_event.getCount());
		build.sendEvent(Main.metric.meter_sender_event.getCount());
		build.ringbuffPack(Main.metric.counter_ringbuff_pack.getCount());
		build.ringbuffEvent(Main.metric.counter_ringbuff_event.getCount());
		ZkUtil.updateCount(offlineId, build.build());
		if (isover || (Main.context.getOverXid() > 0 && Main.context.getOverXid() == Main.context.getSendXid())) {
			TaskOffline buidlTaskOffline = ZkUtil.buidlTaskOffline(offlineId);
			buidlTaskOffline.setRunEndTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
			ZkClient.getInst().createOrUpdateNode(ZkPath.tasksofflines.getPath(buidlTaskOffline.getId()),
					JSONObject.toJSONString(buidlTaskOffline));
			log.info("离线解析完成退出");
			LoggerUtil.exit(JvmStatus.s15);
		}
	}
}
