package net.wicp.tams.duckula.task.parser;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.List;

import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.GrokObj;
import net.wicp.tams.common.apiext.PwdUtil;
import net.wicp.tams.common.binlog.parser.LogEvent;
import net.wicp.tams.common.binlog.parser.event.GtidLogEvent;
import net.wicp.tams.common.binlog.parser.event.QueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.RowsLogBuffer;
import net.wicp.tams.common.binlog.parser.event.RowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.TableMapLogEvent;
import net.wicp.tams.common.binlog.parser.event.TableMapLogEvent.ColumnInfo;
import net.wicp.tams.common.binlog.parser.event.XidLogEvent;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.duckula.common.beans.ColHis;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.plugin.beans.EventTable;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.bean.EventPackage;

@Slf4j
public abstract class BaseLogFetcher {

	protected final IProducer producer;// 在初始化时要初始化它
	protected String fileName = "mysql-bin.000001";
	protected Charset charset = Charset.forName("utf-8");
	protected String gtids;

	private GrokObj gm = GrokObj.getInstance();
	{
		gm.addPattern("tb", "[A-Za-z0-9_.-:]+");
		gm.addPattern("tball", "alter\\s+table\\s+`?%{tb}`?");
	}

	protected BaseLogFetcher(IProducer producer) {
		this.producer = producer;
	}

	protected void parseQueryEvent(QueryLogEvent event) {
		String db = event.getDbName();
		String sql = event.getQuery().toLowerCase();
		if (sql.startsWith("alter")) {
			try {
				Match match = gm.match("%{tball}", sql);
				String tb = String.valueOf(match.toMap().get("tb"));
				tb=tb.replace(db+".", "");//有可能带db进行修改				
				Rule rule = Main.context.findRule(db, tb);
				if (rule == null) {// 没有匹配规则，直接跳过
					return;
				}
				String key = String.format("%s|%s", db, tb).toLowerCase();
				if (Main.context.getColsMap().containsKey(key)) {
					Main.context.addCols(db, tb, event.getWhen());
					log.warn("db:{},tb:{},cols is altered", db, tb);
				} else {
					Main.context.addCols(db, tb, Main.context.getBeginWhen());
					log.info("db:{},tb:{},cols add in map", db, tb);
				}
			} catch (GrokException e) {
				log.error("get tb from sql error", e);
			}
		}
	}

	protected void parseGtidLogEvent(GtidLogEvent event) throws Exception {
		this.gtids = event.getGtid();
		parseGtidLogEventSub(event);
	}

	protected abstract void parseGtidLogEventSub(GtidLogEvent event);

	// 关闭相关资源
	public abstract void close();

	/***
	 * 开始读binlog
	 */
	public abstract void read();
	
	protected long xid;// 当前的GTID

	protected void parseXidEvent(XidLogEvent event) throws Exception {
		EventPackage eventDbsncBuild = producer.getNextBuild();
		//eventDbsncBuild.setXid(true);
		eventDbsncBuild.setXid(event.getXid());
		this.xid=event.getXid();
		producer.sendMsg(eventDbsncBuild);// 结束时发一个空的bean表达结束
	}

	protected boolean parseRowsEvent(RowsLogEvent event, OptType optType) {
		Rule rule = Main.context.findRule(event.getTable().getDbName(), event.getTable().getTableName());
		if (log.isDebugEnabled()) {
			Date d = new Date(event.getHeader().getWhen() * 1000);
			String datestr = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(d);
			log.debug("db:{},tb:{},time:{}", event.getTable().getDbName(), event.getTable().getTableName(), datestr);
		}
		if (rule == null) {// 没有匹配规则，直接跳过
			return false;
		}
		Main.metric.meter_parser_pack_row.mark();
		EventPackage eventDbsncBuild = producer.getNextBuild();
		eventDbsncBuild.setXid(-1l);
		eventDbsncBuild.setRule(rule);

		// 组装位点信息
		Pos pos = new Pos();
		pos.setFileName(fileName);
		pos.setGtids(this.gtids);
		pos.setMasterServerId(Main.context.getParsePos().getMasterServerId());
		pos.setPos(event.getLogPos());
		pos.setTime(event.getHeader().getWhen());
		eventDbsncBuild.setPos(pos);
		// 组装公共信息
		EventTable eventTable = new EventTable();
		eventTable.setDb(event.getTable().getDbName());
		eventTable.setTb(event.getTable().getTableName());
		eventTable.setGtid(this.gtids);
		eventTable.setOptType(optType);
		eventTable.setColsNum(event.getTable().getColumnCnt());
		// 设置列名
		ColHis colhis = Main.context.ValidKey(event.getTable().getDbName(), event.getTable().getTableName(),
				event.getWhen());
		eventTable.setCols(colhis.getCols());

		ColumnInfo[] colsColumnInfo = event.getTable().getColumnInfo();
		int[] colsTypes = new int[colsColumnInfo.length];
		for (int i = 0; i < colsColumnInfo.length; i++) {
			colsTypes[i] = colsColumnInfo[i].type;
		}
		eventTable.setColsType(colsTypes);
		List<String[]> rowListBefore = new ArrayList<>();
		List<String[]> rowListAfter = new ArrayList<>();
		int rows = 0;
		try {
			RowsLogBuffer buffer = event.getRowsBuf(charset.name());
			BitSet columns = event.getColumns();
			BitSet changeColumns = event.getChangeColumns();
			while (buffer.nextOneRow(columns)) {
				// 处理row记录
				int type = event.getHeader().getType();
				if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
					// insert的记录放在before字段中
					rowListAfter.add(parseOneRow(event, buffer, columns, true));
				} else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
					// delete的记录放在before字段中
					rowListBefore.add(parseOneRow(event, buffer, columns, false));
				} else {
					// update需要处理before/after
					// System.out.println("-------> before");
					rowListBefore.add(parseOneRow(event, buffer, columns, true));
					if (!buffer.nextOneRow(changeColumns)) {
						break;
					}
					// System.out.println("-------> after");
					rowListAfter.add(parseOneRow(event, buffer, changeColumns, true));
				}
				rows++;
			}
		} catch (Exception e) {
			throw new RuntimeException("parse row data failed.", e);
		}

		String[][] afterArray = new String[rowListAfter.size()][];
		for (int i = 0; i < rowListAfter.size(); i++) {
			afterArray[i] = rowListAfter.get(i);
		}

		String[][] beforeArray = new String[rowListBefore.size()][];
		for (int i = 0; i < rowListBefore.size(); i++) {
			beforeArray[i] = rowListBefore.get(i);
		}
		eventDbsncBuild.setAfters(afterArray);
		eventDbsncBuild.setBefores(beforeArray);
		eventDbsncBuild.setEventTable(eventTable);
		eventDbsncBuild.setRowsNum(rows);
		producer.sendMsg(eventDbsncBuild);
		Main.metric.meter_parser_event.mark(rows);
		Main.metric.counter_ringbuff_pack.inc();// 包数
		Main.metric.counter_ringbuff_event.inc(rows);// 记录数
		Main.context.setLastPos(event.getHeader().getLogPos());// 解析完了并不意味着发送时的位点。
		return true;
	}

	protected String[] parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter)
			throws UnsupportedEncodingException {
		TableMapLogEvent map = event.getTable();
		if (map == null) {
			throw new RuntimeException("not found TableMap with tid=" + event.getTableId());
		}
		String[] values = new String[event.getTable().getColumnCnt()];
		final int columnCnt = map.getColumnCnt();
		final ColumnInfo[] columnInfo = map.getColumnInfo();
		for (int i = 0; i < columnCnt; i++) {
			if (!cols.get(i)) {
				continue;
			}
			ColumnInfo info = columnInfo[i];
			buffer.nextValue(info.type, info.meta);

			if (buffer.isNull()) {
				//
			} else {
				final Serializable value = buffer.getValue();
				if (value instanceof byte[]) {
					values[i] = PwdUtil.base64FromBin((byte[]) value);// new
																		// String((byte[])
																		// value);
				} else {
					values[i] = String.valueOf(value);
				}
			}
		}
		return values;
	}
}
