package net.wicp.tams.duckula.dump.elasticsearch;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.commons.lang3.ArrayUtils;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkProcessor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.bean.SettingsBean;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;
import net.wicp.tams.common.metrics.utility.TsLogger;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.dump.elasticsearch.bean.EventDump;
import net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer.BaseDataHander;
import net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer.BusiHander;
import net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer.Publisher;
import net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer.SendHander;
import net.wicp.tams.duckula.dump.elasticsearch.jmx.DumpControl;

/***
 * 消费模式
 * 
 * @author 偏锋书生
 *
 *         2018年5月27日
 */
@Slf4j
public class MainDump {
	public static DumpGroup metric;
	public static Publisher publisher;
	private static final int BUFFER_SIZE = 128 * 1; // 1024 3G以上 256 2.8G 128 2G

	// private final ExecutorService executor;// = Executors.newFixedThreadPool(8,
	// DaemonThreadFactory.INSTANCE);

	private final EventFactory<EventDump> EVENT_FACTORY = new EventFactory<EventDump>() {
		public EventDump newInstance() {
			return new EventDump();
		}
	};

	private final RingBuffer<EventDump> ringBuffer = createMultiProducer(EVENT_FACTORY, BUFFER_SIZE,
			new BlockingWaitStrategy());

	public void init(String[] args) throws SQLException {
		Thread.currentThread().setName("Dump-main");
		if (ArrayUtils.isEmpty(args)) {
			System.err.println("----未传入taskid，不能启动task----");
			log.error("----未传入taskid，不能启动task----");
			return;
		}
		dumpId = args[0];
		log.info("----------------------加载配置文件-------------------------------------");
		CommandType.dump.setCommonProps();
		//加载es
		Dump dump = ZkUtil.buidlDump(dumpId);
		Properties configMiddleware = ConfUtil.configMiddleware(MiddlewareType.es, dump.getCluster());
		Conf.overProp(configMiddleware);		
		log.info("----------------------创建zk临时文件-------------------------------------");
		String curtimestr = DateFormatCase.yyyyMMddHHmmss.getInstanc().format(new Date());
		String tempNodePath = IOUtil.mergeFolderAndFilePath(ZkPath.dumps.getPath(dumpId), curtimestr);
		String ip = "unknow";
		try {
			InetAddress addr = OSinfo.findFirstNonLoopbackAddress();
			ip = addr.getHostAddress().toString(); // 获取本机ip
		} catch (Exception e) {
			log.error("获取本机IP失败");
		}
		ZkClient.getInst().createNode(tempNodePath, ip, true);
		log.info("----------------------启动jmx-------------------------------------");
		try {
			initMbean();// 启动jxmx
		} catch (Exception e) {
			log.error("启动jmx错误", e);
			LoggerUtil.exit(JvmStatus.s15);
		}
		log.info("----------------------配置metrix-------------------------------------");
		System.setProperty(TsLogger.ENV_FILE_NAME, "dump_" + dumpId);
		System.setProperty(TsLogger.ENV_FILE_ROOT, String.format("%s/logs/metrics", System.getenv("DUCKULA_DATA")));
		metric = new DumpGroup(dumpId);
		log.info("----------------------导入配置-------------------------------------");
		
		Task task = ZkUtil.buidlTask(dump.getTaskOnlineId());
		Properties props = new Properties();
		props.put("common.jdbc.datasource.host", task.getIp());
		if (StringUtil.isNotNull(task.getDefaultDb())) {
			props.put("common.jdbc.datasource.defaultdb", task.getDefaultDb());
		} else {
			props.put("common.jdbc.datasource.defaultdb", "none");
		}
		if (task.getIsSsh() != null && task.getIsSsh() == YesOrNo.yes) {
			props.put("common.jdbc.ssh.enable", "true");
		} else {
			props.put("common.jdbc.ssh.enable", "false");
		}
		props.put("common.jdbc.datasource.port", String.valueOf(task.getPort()));
		props.put("common.jdbc.datasource.username", task.getUser());
		props.put("common.jdbc.datasource.password", task.getPwd());
		Conf.overProp(props);
		log.info("----------------------启动Disruptor-------------------------------------");
		String startId = args.length > 1 ? args[1] : null;
		long recordNum = Long.parseLong(args.length > 2 ? args[2] : "-1");
		try {
			disruptorRun(dump, startId, recordNum);
		} catch (Exception e) {
			log.error("dump失败,将关机，原因：",e);
			LoggerUtil.exit(JvmStatus.s15);
		}
		addTimer();
	}

	private String dumpId;
	SequenceBarrier baseBarrier;
	SequenceBarrier busiBarrier;

	private void disruptorRun(Dump dump, String startId, long recordNum) {
		// 如果保证顺序需要单线程
		Publisher[] valuePublishers = new Publisher[1];
		for (int i = 0; i < valuePublishers.length; i++) {
			valuePublishers[i] = new Publisher(ringBuffer, dump, startId, recordNum);
		}
		publisher = valuePublishers[0];
		///////////////////////////////// 取基础数据////////////////////////////////////
		Mapping mapping = ZkUtil.buidlMapping(dump.getMappingId());
		Sequence workSequence = new Sequence(-1);
		SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
		int baseDataNum = dump.getBaseDataNum() == null ? 2 : dump.getBaseDataNum();
		BaseDataHander[] baseDataHanders = new BaseDataHander[baseDataNum];
		for (int i = 0; i < baseDataHanders.length; i++) {
			baseDataHanders[i] = new BaseDataHander(dump, mapping);
		}
		@SuppressWarnings("unchecked")
		WorkProcessor<EventDump>[] baseProcessors = new WorkProcessor[baseDataNum];
		for (int i = 0; i < baseProcessors.length; i++) {
			baseProcessors[i] = new WorkProcessor<EventDump>(ringBuffer, sequenceBarrier, baseDataHanders[i],
					new IgnoreExceptionHandler(), workSequence);
		}
		///////////////////////////////////////// 业务处理///////////////////////////////////////////////////////////////////////////////
		baseBarrier = ringBuffer.newBarrier(getSeqAry(baseProcessors));
		Sequence busiSequence = new Sequence(-1);
		int busiNum = dump.getBusiNum() == null ? 2 : dump.getBusiNum();
		BusiHander[] busiHanders = new BusiHander[busiNum];
		for (int i = 0; i < busiHanders.length; i++) {
			busiHanders[i] = new BusiHander(dump);
		}
		@SuppressWarnings("unchecked")
		WorkProcessor<EventDump>[] busiProcessors = new WorkProcessor[busiNum];
		for (int i = 0; i < busiProcessors.length; i++) {
			busiProcessors[i] = new WorkProcessor<EventDump>(ringBuffer, baseBarrier, busiHanders[i],
					new IgnoreExceptionHandler(), busiSequence);
		}

		///////////////////////////////////////// 发送处理///////////////////////////////////////////////////////////////////////////////
		busiBarrier = ringBuffer.newBarrier(getSeqAry(busiProcessors));
		Sequence sendSequence = new Sequence(-1);
		//不需要发送只要一个线程就OK了
		int sendNum =( dump.getNeedSend()!=null&&dump.getNeedSend()==YesOrNo.no)?1:(dump.getSendNum() == null ? 2 : dump.getSendNum());
		SendHander[] sendHanders = new SendHander[sendNum];
		for (int i = 0; i < sendHanders.length; i++) {
			sendHanders[i] = new SendHander(mapping,dump.getNeedSend());
		}
		@SuppressWarnings("unchecked")
		WorkProcessor<EventDump>[] sendProcessors = new WorkProcessor[sendNum];
		for (int i = 0; i < sendProcessors.length; i++) {
			sendProcessors[i] = new WorkProcessor<EventDump>(ringBuffer, busiBarrier, sendHanders[i],
					new IgnoreExceptionHandler(), sendSequence);
		}
		/////////////////////////////////// 关闭自动刷新(如果性能可以不用关闭)////////////////////////////////////////////////////////
		//ESClientOnlyOne.getInst().getESClient().indexSetting(mapping.getIndex(),
		//		SettingsBean.builder().refresh_interval(SettingsBean.fresh_0).build());
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		ringBuffer.addGatingSequences(getSeqAry(sendProcessors));
		ExecutorService executor = Executors.newFixedThreadPool(1 + baseDataNum + busiNum + sendNum,
				DaemonThreadFactory.INSTANCE);
		for (int i = 0; i < valuePublishers.length; i++) {
			executor.submit(valuePublishers[i]);
		}
		for (WorkProcessor<EventDump> baseProcessor : baseProcessors) {
			executor.submit(baseProcessor);
		}
		for (WorkProcessor<EventDump> busiProcessor : busiProcessors) {
			executor.submit(busiProcessor);
		}
		for (WorkProcessor<EventDump> sendProcessor : sendProcessors) {
			executor.submit(sendProcessor);
		}

		addShutdownHook(mapping);
	}

	private Sequence[] getSeqAry(WorkProcessor<EventDump>[] baseProcessors) {
		Sequence[] seqAry = new Sequence[baseProcessors.length];
		for (int i = 0; i < seqAry.length; i++) {
			seqAry[i] = baseProcessors[i].getSequence();
		}
		return seqAry;
	}

	private void addShutdownHook(Mapping mapping) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("----------------------执行关闭进程 钩子开始-------------------------------------");
				// DisruptorManager.getInst().stop(); // 为什么hold住？
				updateLastId();
				ESClientOnlyOne.getInst().getESClient().indexSetting(mapping.getIndex(),
						SettingsBean.builder().refresh_interval(SettingsBean.fresh_null).build());
				log.info("----------------------执行关闭进程 钩子完成-------------------------------------");
			}
		});
	}

	private static void initMbean() throws InstanceAlreadyExistsException, MBeanRegistrationException,
			NotCompliantMBeanException, MalformedObjectNameException {
		DumpControl control = new DumpControl();
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		mbs.registerMBean(control, new ObjectName("Commons:name=" + Conf.get("duckula.dump.mbean.beanname")));
		log.info("----------------------MBean注册成功-------------------------------------");
	}

	private void addTimer() {
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
		service.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				updateLastId();
			}
		}, 10, 3, TimeUnit.SECONDS);
	}

	private void updateLastId() {
		long min = this.ringBuffer.getMinimumGatingSequence();
		long timeUse = System.currentTimeMillis() - MainDump.publisher.getTimeBegin();
		log.info(
				"-------------------------speed:{},time:{} minute,undo size:{},sendDuanNo:{},busiDuanNo:{},baseDateDuanNo:{},publisher:{}",
				MainDump.metric.counter_send_es.getCount() * 1000 / timeUse, timeUse / (1000 * 60),
				ringBuffer.getCursor() - min, min, busiBarrier.getCursor(), baseBarrier.getCursor(),
				ringBuffer.getCursor());
		String lastId = this.ringBuffer.get(min).getEndId();
		if (StringUtil.isNotNull(lastId)) {
			String path = IOUtil.mergeFolderAndFilePath(ZkPath.dumps.getPath(dumpId), "lastId");
			ZkClient.getInst().createOrUpdateNode(path, lastId);
		}
	}

	public static void main(String[] args) throws IOException, SQLException {
		MainDump main = new MainDump();
		main.init(args);
		System.in.read();
	}
}
