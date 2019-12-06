package net.wicp.tams.duckula.task.parser;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.binlog.parser.DirectLogFetcher;
import net.wicp.tams.common.binlog.parser.LogContext;
import net.wicp.tams.common.binlog.parser.LogDecoder;
import net.wicp.tams.common.binlog.parser.LogEvent;
import net.wicp.tams.common.binlog.parser.event.DeleteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.GtidLogEvent;
import net.wicp.tams.common.binlog.parser.event.QueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.RotateLogEvent;
import net.wicp.tams.common.binlog.parser.event.UpdateRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.WriteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.XidLogEvent;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.bean.GtidBean;
import net.wicp.tams.duckula.task.constant.Checksum;

@Slf4j
public class ParseLogOnline extends BaseLogFetcher {

	private final Connection conn;
	private final String uuid;
	private String slaveGtids;// 从服务器第一个字符为","

	private final boolean gtidCan;

	private DirectLogFetcher fecther;

	public ParseLogOnline(IProducer producer) {
		super(producer);
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					String.format("jdbc:mysql://%s:%s", Main.context.getTask().getIp(),
							Main.context.getTask().getPort()),
					Main.context.getTask().getUser(), Main.context.getTask().getPwd());

			Statement statement = conn.createStatement();
			statement.execute("SET @master_binlog_checksum='@@global.binlog_checksum'");
			statement.execute("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
			statement.close();

			uuid = getVar(conn, "server_uuid", false);
			super.gtidBean = GtidBean.builder().gtids(Main.context.getParsePos().getGtids())
					.commitTime(Main.context.getParsePos().getTime()).build();
			fileName = Main.context.getParsePos().getFileName();
			// 设置一些参数
			Checksum checksum = Checksum.get(getVar(conn, "binlog_checksum", false));
			initSlaveGtids(Main.context.getParsePos().getGtids());

			Main.context.setChecksum(checksum);
			gtidCan = isAvailable(conn, Main.context.getParsePos().getGtids(), uuid);// 可以使用gtid
			log.info("gtid：[{}]是否可用：[{}]", Main.context.getParsePos().getGtids(), gtidCan);
		} catch (Exception e) {
			log.error("读binlog日志出现问题", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void read() {
		if (Main.context.getParsePos().getPos() < 4) {
			throw new IllegalAccessError("开始位置最小为4");
		}
		fecther = new DirectLogFetcher();
		try {
			fetchLog();
			//
			LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
			LogContext context = new LogContext();
			boolean isSel = false;
			while (fecther.fetch()) {
				Main.metric.meter_parser_pack_all.mark();// 总包数
				LogEvent event = null;
				event = decoder.decode(fecther, context);
				if (event == null) {
					continue;
					// throw new RuntimeException("parse failed");
				}

				int eventType = event.getHeader().getType();
				switch (eventType) {
				case LogEvent.FORMAT_DESCRIPTION_EVENT: // MySQL
														// Server的版本，binlog的版本，该binlog文件的创建时间
					Main.context.setBeginWhen(event.getWhen());// 设置开始时间，为存title用
					break;
				case LogEvent.ROTATE_EVENT:
					fileName = ((RotateLogEvent) event).getFilename();
					break;
				case LogEvent.WRITE_ROWS_EVENT_V1:
				case LogEvent.WRITE_ROWS_EVENT:
					if (super.gtidBean.getGtids() != null
							&& parseRowsEvent((WriteRowsLogEvent) event, OptType.insert)) {
						isSel = true;
					}
					break;
				case LogEvent.UPDATE_ROWS_EVENT_V1:
				case LogEvent.UPDATE_ROWS_EVENT:
					if (super.gtidBean.getGtids() != null
							&& parseRowsEvent((UpdateRowsLogEvent) event, OptType.update)) {
						isSel = true;
					}
					break;
				case LogEvent.DELETE_ROWS_EVENT_V1:
				case LogEvent.DELETE_ROWS_EVENT:
					if (super.gtidBean.getGtids() != null
							&& parseRowsEvent((DeleteRowsLogEvent) event, OptType.delete)) {
						isSel = true;
					}
					break;
				case LogEvent.QUERY_EVENT:
					parseQueryEvent((QueryLogEvent) event);
					break;
				case LogEvent.XID_EVENT:
					if (super.gtidBean.getGtids() != null && isSel) {
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
		} catch (SQLException e) {// 得到联接错误
			log.error("得到联接错误", e);
		} catch (IOException e) {// 拉取日志文件错误
			if (e.getMessage().contains("errno = 1236, sqlstate = HY000")) {
				// TODO 偿试处理要处理的位点被删除的错误
			}
			log.error("拉取日志文件错误", e);
		} catch (Throwable e) {// 其它错误
			log.error("未知错误", e);
		} finally {
			try {
				fecther.close();
			} catch (IOException e) {
				log.error("关闭fecther失败", e);
			}
		}

	}

	private void fetchLog() throws IOException, CloneNotSupportedException {
		boolean useGtid = true;
		boolean needSecond = false;
		try {
			if (gtidCan && !Main.context.getParsePos().isIshalf()) {
				String gtidTrue = super.gtidBean.getGtids();
				if (Main.context.getTask().getSenderEnum().isIdempotent()) {// 是全幂等模式，需要回推多少位点？否则会有数据丢失风险:如果在停机时kafka未发送完

					//gtidTrue = backGtid(gtidTrue, 16 * 3);// TODO 要回溯多少个位点不知，先设置48个//手动设置回退5分钟，不用回退
				}
				log.info("star from the gtid:{}",gtidTrue);
				fecther.openGtid(conn, gtidTrue, Main.context.getTask().getClientId());
				// fecther.openGtid(conn,
				// "f07e8023-5c57-11e6-ad03-340286ad00d3:1-115",
				// Main.context.getTask().getClientId());// 本机1000W数据
				// fecther.openGtid(conn,
				// "1d17d2f2-fbd1-11e5-b794-008cfae413f8:581770205,4f6e51f7-fbd1-11e5-b795-008cfae412f8:1-280589239",
				// Main.context.getTask().getClientId());// 测试1000W数据
				// fecther.open(conn, "mysql_bin.000023", 4L, 2);
			} else {
				useGtid = false;
				fetchLogFroPos();
			}
		} catch (Exception e) {
			log.error("第一次启动失败，使用gtid:" + useGtid, e);
			needSecond = true;
		}
		if (needSecond) {// 偿试第二次换种方式启动
			if (useGtid) {
				fetchLogFroPos();
			} else {
				fecther.openGtid(conn, super.gtidBean.getGtids(), Main.context.getTask().getClientId());
			}
		}
		// 连接成功后设置发送位点
		Pos parsePos = Main.context.getParsePos().clone();
		Main.context.setPos(parsePos);
		Main.context.setLastPos(parsePos.getPos());// 还没有符合条件的数据进来时,没有它在Main更新位点时就会出问题
	}

	private void fetchLogFroPos() throws IOException {
		if (Main.context.getInitPos() != null) {
			int upPos = 10000;
			Main.context.getParsePos()
					.setPos(Main.context.getParsePos().getPos() > upPos + 4
							? (Main.context.getParsePos().getPos() - upPos)
							: Main.context.getParsePos().getPos());
			log.warn("已做主备切换，但不支持Gtid，采用回朔10000位点的方式");
		}
		fecther.open(conn, Main.context.getParsePos().getFileName(), Main.context.getParsePos().getPos(),
				Main.context.getTask().getClientId());
	}

	@Override
	protected void parseGtidLogEventSub(GtidLogEvent event) {
		if (StringUtil.isNotNull(uuid) && uuid.equals(event.getSource())) {// 当做主备时会变化source
			if (StringUtil.isNotNull(slaveGtids)) {
				super.gtidBean = GtidBean.builder().gtids(String.format("%s,%s", super.gtidBean.getGtids(), slaveGtids))
						.commitTime(event.getWhen()).build();
				// super.gtids = String.format("%s,%s", super.gtids, slaveGtids);
			}
			// 设置解析的位点
			Main.context.getParsePos().setGtids(super.gtidBean.getGtids());
			Main.context.getParsePos().setPos(event.getLogPos());
			Main.context.getParsePos().setTime(super.gtidBean.getCommitTime());
			Main.context.getParsePos().setIshalf(false);// 解析位点不用区分是否一半
		} else {
			log.info("------------------做主备切换,原主机源[{}],切换源[{}}]--------------------------------", uuid,
					event.getSource());
			super.gtidBean = null;
		}
	}

	@Override
	public void close() {
		if (fecther != null) {
			try {
				fecther.close();
			} catch (IOException e) {
				log.error("关闭fecther失败", e);
			}
		}
		producer.stop();// 停止一线线程
		LoggerUtil.exit(JvmStatus.s15);
	}

	///////////////////////

	public String getVar(Connection conn, String var, boolean isGlobal) throws SQLException {
		String sql = String.format("select @@%s%s", isGlobal ? "GLOBAL." : "", var);
		ResultSet resultSet = JdbcAssit.querySql(conn, sql);
		resultSet.next();
		String retstr = resultSet.getString(1);
		resultSet.close();
		return retstr;
	}

	public boolean isAvailable(Connection conn, String gtidStr, String uuid) throws SQLException {
		if (StringUtil.isNull(gtidStr)) {// 都没传gtid，说明不走gtid
			return false;
		}
		long maxGtidDel = maxGtid(getVar(conn, "GTID_PURGED", true), uuid);
		long maxGtidCur = maxGtid(gtidStr.replace("\n", ""), uuid);
		if (maxGtidDel == 0 || (maxGtidDel > 0 && maxGtidCur > 0 && maxGtidCur > maxGtidDel)) {
			return true;
		} else {
			return false;
		}
	}

	private long maxGtid(String gtidStr, String uuid) {
		if (StringUtil.isNull(gtidStr)) {// 本机在@@GLOBAL.GTID_PURGED为“”的情况
			return 0;
		}
		String[] gtidAry = gtidStr.split(",");
		long delmax = 0;
		for (String eleGtid : gtidAry) {
			if (eleGtid.startsWith(uuid)) {
				int index1 = eleGtid.lastIndexOf(":");
				String[] nums = eleGtid.substring(index1 + 1).split("-");
				String delNumStr = nums.length > 1 ? nums[1] : nums[0];
				delmax = Long.parseLong(delNumStr);
				break;
			}
		}
		return delmax;
	}

	private void initSlaveGtids(String gtids) {
		if (StringUtil.isNotNull(gtids)) {
			super.gtidBean = GtidBean.builder().gtids(gtids.replace("\n", "")).build();
			String[] gtidsAry = super.gtidBean.getGtids().split(",");

			for (String gtid : gtidsAry) {
				if (gtid.startsWith(uuid)) {
					gtidsAry = (String[]) ArrayUtils.removeElement(gtidsAry, gtid);
					break;
				}
			}
			this.slaveGtids = CollectionUtil.arrayJoin(gtidsAry, ",");
		}
	}

	private String backGtid(String gtids, int gtidNum) {
		if (StringUtil.isNotNull(gtids)) {
			GtidBean tempBean = GtidBean.builder().gtids(gtids.replace("\n", "")).build();
			String[] gtidsAry = tempBean.getGtids().split(",");
			for (int i = 0; i < gtidsAry.length; i++) {
				String gtid = gtidsAry[i];
				if (gtid.startsWith(uuid)) {
					String[] gtidAry = gtid.split(":");
					String[] ids = gtidAry[1].split("-");
					int parseInt = Integer.parseInt(ids[ids.length - 1]);
					gtidsAry[i] = String.format("%s:%s-%s", gtidAry[0], 1,
							parseInt > gtidNum ? parseInt - gtidNum : parseInt);
					break;
				}
			}
			return CollectionUtil.arrayJoin(gtidsAry, ",");
		}
		return gtids;
	}

}
