package net.wicp.tams.duckula.task.disruptor;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.EventHandler;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.constant.SerializerEnum;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.IReceiver;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;
import net.wicp.tams.duckula.plugin.serializer.SerializerAssit;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.bean.EventPackage;

/***
 * 此为单线程任务，要保证此线程不出异常
 * 
 * @author rjzjh
 *
 */
@Slf4j
public class DisruptorSendHandler implements EventHandler<EventPackage> {
	private static final org.slf4j.Logger errorBinlog = org.slf4j.LoggerFactory.getLogger("errorBinlog");
	private final IReceiver receive;
	private final ISerializer serialize;
	private final ExecutorService exec;
	private final int sendAll = 8;// 总共重试次数 最大128S,总共256S
	public static File rootDir;
	static {
		rootDir = new File(System.getenv("DUCKULA_DATA"));
	}
	// private final boolean isSync;// 需要同步实现,现只有ES插件需要。

	public DisruptorSendHandler(JSONObject params) {
		Thread.currentThread().setName("duckula-sendHandler");
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		Plugin serializerPlugin = null;
		SerializerEnum serializerEnum = Main.context.getTask().getSerializerEnum();
		if (serializerEnum != null && serializerEnum != SerializerEnum.no) {
			serializerPlugin = pluginAssit.newPlugin(
					IOUtil.mergeFolderAndFilePath(rootDir.getPath(),
							Main.context.getTask().getSerializerEnum().getPluginJar()),
					"net.wicp.tams.duckula.plugin.serializer.ISerializer", classLoader,
					"net.wicp.tams.duckula.plugin.serializer.ISerializer");
			Thread.currentThread().setContextClassLoader(serializerPlugin.getLoad().getClassLoader());// 需要加载前设置好classload
			this.serialize = SerializerAssit.loadSerialize(serializerPlugin);
		} else {
			this.serialize = null;
		}

		Plugin plugin = pluginAssit.newPlugin(
				IOUtil.mergeFolderAndFilePath(rootDir.getPath(), Main.context.getTask().getReceivePluginDir()),
				"net.wicp.tams.duckula.plugin.receiver.IReceiver",
				serializerPlugin == null ? classLoader : serializerPlugin.getLoad().getClassLoader(),
				"net.wicp.tams.duckula.plugin.receiver.IReceiver", "net.wicp.tams.duckula.plugin.receiver.ReceiveAbs");
		Thread.currentThread().setContextClassLoader(plugin.getLoad().getClassLoader());// 需要加载前设置好classload
		this.receive = ReceiveAbs.loadReceive(plugin, params);
		if (receive == null) {
			log.error("加载接收器失败");
			LoggerUtil.exit(JvmStatus.s15);
		}

		if (receive.isSync()) {// 不是中间存储，是最终存储，现在只有ES
			exec = null;
			ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
			// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
			service.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					if (sendBeginTime == 0) {
						return;
					}
					if (System.currentTimeMillis() - sendBeginTime > 5 * 60 * 1000) {// 五分种没有发送完成，自杀
						log.error("五分种没有发送完成，自杀。");
						LoggerUtil.exit(JvmStatus.s15);
					}
				}
			}, 10, 30, TimeUnit.SECONDS);

		} else {
			exec = Executors.newFixedThreadPool(1);
		}
	}

	private volatile long sendBeginTime;

	private Pos curPos = null;

	@Override
	public void onEvent(final EventPackage event, long sequence, boolean endOfBatch) throws Exception {
		if (event.getXid() > 0) {
			// curPos==null 发生在没有更新数据，却有表结构修改的情况下
			if (curPos != null) {
				// 更新最新gtid
				curPos.setIshalf(false);
				Main.context.setPos(curPos);
			}
			Main.context.setSendXid(event.getXid());// 不能在此处判断 overXid是否相等而关闭虚拟机，因为它会比 setOverXid先执行。
			return;
		}
		// log.info("send
		// status:{},record:{}",event.isOver(),ArrayUtils.isEmpty(event.getAfters())?"":event.getAfters()[0][0]);
		if (event.isOver()) {// 处理的时候出现问题或无需处理，一般为业务处理失败
			if (curPos != null) {
				Main.context.setPos(curPos);
				updatePos(event, false);// 20190813 没有记录也得更新位点
			}
			return;
		}
		// log.info("send:{}", event.getRowsNum());
		// 真正发送
		int sendNum = 0;
		if (receive.isSync()) {
			sendBeginTime = System.currentTimeMillis();
			while (true) {
				long timeBegin = System.currentTimeMillis();
				try {
					Boolean retvalue = null;
					if (serialize == null) {
						retvalue = receive.receiveMsg(event, event.getRule());
					} else {
						retvalue = receive.receiveMsg(
								serialize.serialize(event, event.getRule().getItems().get(RuleItem.splitkey)),
								event.getRule());
					}
					if (retvalue != null && retvalue.booleanValue()) {
						updatePos(event, true);
						sendBeginTime = 0;// 不进入计时阶段
						return;
					} else if (sendNum < sendAll) {
						log.error("第[{}]次发送失败，等待时间：[{}].请联系相关人员。", sendNum, System.currentTimeMillis() - timeBegin);
						sendNum++;
					} else {
						if (Main.context.isSync()) {
							log.error("总共发送[{}]次失败，发送内容[{}] 系统将停止此服务，在另一台机上重试发送", sendAll, event);
							LoggerUtil.exit(JvmStatus.s15);
						} else {
							updatePos(event, true);
							sendBeginTime = 0;
							errorBinlog.error(curPos.toString());
							return;
						}
					}
				} catch (Throwable e) {
					log.error("接收器代码异常，请检查并修改接收器.发送内容：" + event, e);
					LoggerUtil.exit(JvmStatus.s15);
				}
			}
		} else {
			int timeout = 1;
			while (true) {
				Future<Boolean> future = exec.submit(new Callable<Boolean>() {
					public Boolean call() throws Exception {
						if (serialize == null) {
							return receive.receiveMsg(event, event.getRule());
						} else {
							return receive.receiveMsg(
									serialize.serialize(event, event.getRule().getItems().get(RuleItem.splitkey)),
									event.getRule());
						}
					}
				});
				timeout = timeout * 2;
				Boolean retvalue = false;
				try {
					retvalue = future.get(timeout, TimeUnit.SECONDS);// TimeUnit.SECONDS
				} catch (TimeoutException e) {
					future.cancel(true);
				} catch (Throwable th) {
					log.error("接收器代码异常，请检查并修改接收器.发送内容：" + event, th);
					LoggerUtil.exit(JvmStatus.s15);
				}

				if (retvalue != null && retvalue.booleanValue()) {
					updatePos(event, true);
					break;
				} else if (sendNum < sendAll) {
					log.error("第[{}]次发送失败，等待时间：[{}].请联系相关人员。", sendNum, timeout);
					sendNum++;
				} else {
					if (Main.context.isSync()) {
						log.error("总共发送[{}]次失败，发送内容[{}] 系统将停止此服务，在另一台机上重试发送", sendAll, event);
						LoggerUtil.exit(JvmStatus.s15);
					} else {
						updatePos(event, true);
						errorBinlog.error(curPos.toString());
						break;
					}
				}
			}
		}

	}

	private void updatePos(final EventPackage event, boolean suc) {
		curPos = event.getPos();
		Main.context.getPos().setFileName(event.getPos().getFileName());
		Main.context.getPos().setPos(event.getPos().getPos());
		Main.context.getPos().setTime(event.getPos().getTime());
		Main.context.getPos().setMasterServerId(event.getPos().getMasterServerId());
		Main.context.getPos().setIshalf(true);
		// 增加统计
		Main.metric.counter_ringbuff_pack.dec();
		Main.metric.counter_ringbuff_event.dec(event.getRowsNum());
		Main.metric.meter_sender_pack.mark();
		Main.metric.meter_sender_event.mark(event.getRowsNum());

		if (suc) {
			Main.metric.meter_dowith_event.mark(event.getRowsNum());
			switch (event.getEventTable().getOptType()) {
			case insert:
				Main.metric.meter_sender_event_add.mark(event.getRowsNum());

				break;

			case delete:
				Main.metric.meter_sender_event_del.mark(event.getRowsNum());
				break;

			case update:
				Main.metric.meter_sender_event_update.mark(event.getRowsNum());
				break;

			default:
				break;
			}
		}
	}
}
