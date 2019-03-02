package net.wicp.tams.duckula.task.parser;

import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.binlog.parser.FileLogFetcher;
import net.wicp.tams.common.binlog.parser.GtidSet;
import net.wicp.tams.common.binlog.parser.LogContext;
import net.wicp.tams.common.binlog.parser.LogDecoder;
import net.wicp.tams.common.binlog.parser.LogEvent;
import net.wicp.tams.common.binlog.parser.LogPosition;
import net.wicp.tams.common.binlog.parser.event.DeleteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.GtidLogEvent;
import net.wicp.tams.common.binlog.parser.event.QueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.UpdateRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.WriteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.XidLogEvent;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.task.Main;

@Slf4j
public class ParseLogOffline extends BaseLogFetcher {

	private final FileLogFetcher fetcher = new FileLogFetcher(2 * 1024 * 1024);// 存在156*1024的,继续扩大，存在：1085899（1060*1024
																				// 150多个字段变态表）的,放大它会增加buff的复制成本
	private final TaskOffline taskOffline;



	

	public ParseLogOffline(TaskOffline taskOffline, IProducer producer) {
		super(producer);
		if (taskOffline == null) {
			log.error("the task is null");
			System.err.println("the task is null");
			LoggerUtil.exit(JvmStatus.s15);
		}
		this.taskOffline = taskOffline;
	}

	@Override
	protected void parseGtidLogEventSub(GtidLogEvent event) {
	}

	@Override
	public void read() {
		Result ret = this.checkValid();
		if (!ret.isSuc()) {
			log.error("check error：{}", ret.getMessage());
			System.err.println("check error:" + ret.getMessage());
			return;
		}
		// 初始化pos
		Pos begingPos = new Pos();
		// begingPos.setMasterServerId(taskOffline.getTaskOnline().getDbinst());
		Main.context.setPos(begingPos);
		String[] logfiles = taskOffline.getBinlogFiles().split(",");
		String logDir = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/binlog",
				taskOffline.getDbId());
		for (int i = 0; i < logfiles.length; i++) {
			logfiles[i] = IOUtil.mergeFolderAndFilePath(logDir, logfiles[i]);
		}
		taskOffline.setBinlogFiles(CollectionUtil.arrayJoin(logfiles, ","));
		// taskOffline.setBinlogFiles("D:/duckula/binlog/dev/1150837/mysql-bin.002434");//
		// 测试
		try {
			LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
			LogContext context = new LogContext();
			String[] binlogFiles = taskOffline.getBinlogFiles().split(",");
			for (String fileUrl : binlogFiles) {
				log.info("cur file:{}", fileUrl);
				File file = new File(fileUrl);
				if (taskOffline.getPosBegin() > 0) {
					fetcher.open(file, taskOffline.getPosBegin());
				} else {
					fetcher.open(file, 4L);
				}
				context.setLogPosition(new LogPosition(file.getName()));
				boolean isSel = false;
				while (fetcher.fetch()) {
					Main.metric.meter_parser_pack_all.mark();// 总包数
					LogEvent event = null;
					event = decoder.decode(fetcher, context);
					if (event != null) {
						// TODO gtid处理 && (gtidBegin!=null && 1==1)
						if (taskOffline.getLimitType() == 1 && ((taskOffline.getTimeBegin() != null
								&& event.getWhen() * 1000 < taskOffline.getTimeBegin().getTime())
								|| (taskOffline.getTimeEnd() != null
										&& event.getWhen() * 1000 > taskOffline.getTimeEnd().getTime()))) {
							continue;
						}
						int eventType = event.getHeader().getType();
						switch (eventType) {
						case LogEvent.WRITE_ROWS_EVENT_V1:
						case LogEvent.WRITE_ROWS_EVENT:
							if (super.gtids != null && parseRowsEvent((WriteRowsLogEvent) event, OptType.insert)) {
								isSel = true;
							}
							break;
						case LogEvent.UPDATE_ROWS_EVENT_V1:
						case LogEvent.UPDATE_ROWS_EVENT:
							if (super.gtids != null && parseRowsEvent((UpdateRowsLogEvent) event, OptType.update)) {
								isSel = true;
							}
							break;
						case LogEvent.DELETE_ROWS_EVENT_V1:
						case LogEvent.DELETE_ROWS_EVENT:
							if (super.gtids != null && parseRowsEvent((DeleteRowsLogEvent) event, OptType.delete)) {
								isSel = true;
							}
							break;
						case LogEvent.QUERY_EVENT:
							parseQueryEvent((QueryLogEvent) event);
							break;
						case LogEvent.XID_EVENT:
							if (super.gtids != null && isSel) {
								parseXidEvent((XidLogEvent) event);
								isSel = false;
							}
							break;
						case LogEvent.GTID_LOG_EVENT:
							parseGtidLogEvent((GtidLogEvent) event);
							break;
						default:
							break;
						}
					}
				}
			}
			Main.context.setOverXid(super.xid);
		} catch (Exception e) {
			log.error("parser error", e);
		} finally {
			try {
				fetcher.close();
			} catch (IOException e) {
				log.error("关闭fecther失败", e);
			}
		}
		log.info("exec is over");
	}

	@Override
	public void close() {
		if (fetcher != null) {
			try {
				fetcher.close();
			} catch (IOException e) {
				log.error("关闭fecther失败", e);
			}
		}
		//producer.stop();// 停止一线线程
		//LoggerUtil.exit(JvmStatus.s15, 180 * 1000);// 延迟3分种退出，为了让kafka发送完数据
	}

	private Result checkValid() {
		if (taskOffline.getLimitType() == 1 && taskOffline.getTimeBegin() == null) {
			return Result.getError("时间限制模式需要开始时间");
		}
		if (taskOffline.getLimitType() == 2 && StringUtil.isNull(taskOffline.getGtidBegin())
				&& StringUtil.isNull(taskOffline.getGtidEnd())) {
			return Result.getError("Gtid模式需要开始或结束的Gtid");
		}
		if (taskOffline.getTimeBegin() != null && taskOffline.getTimeEnd() != null
				&& taskOffline.getTimeBegin().after(taskOffline.getTimeEnd())) {
			return Result.getError("开始时间不能大于结束时间");
		}
		if (StringUtil.isNotNull(taskOffline.getGtidBegin()) && StringUtil.isNotNull(taskOffline.getGtidEnd())) {
			GtidSet gtidSet1 = new GtidSet(taskOffline.getGtidBegin());
			GtidSet gtidSet2 = new GtidSet(taskOffline.getGtidEnd());
			if (gtidSet1.compareTo(gtidSet2) > 0) {
				return Result.getError("开始gtid不能大于结束gtid");
			}
		}
		return Result.getSuc();
	}

}
