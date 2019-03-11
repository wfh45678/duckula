package net.wicp.tams.duckula.task.bean;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.common.beans.Host;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.ColHis;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.constant.Checksum;

@Data
@Slf4j
public class DuckulaContext {
	private Host host;
	private Task task;
	private Pos pos;// 发送时的位点
	private Pos parsePos;// 解析的位点
	private long lastPos;// 最后符合条件的位点
	private Checksum checksum;
	private boolean sync = true;// 是否同步发送,如果是异步发送，发送错误时只打日志，不终止发送程序。如果是同步发送出现问题将终止虚似机
	private Pos initPos;// 只有做过主备时此值才会有值
	private long beginWhen;// task支持的binlog解析时间,一般为task任务创建时间

	private long overXid;// 离线解析跟据它决定是否退出虚拟机
	private long sendXid;// 发送的最后xid

	private Map<String, SortedSet<ColHis>> colsMap = new HashMap<>();

	@Getter(value = AccessLevel.PRIVATE)
	@Setter(value = AccessLevel.PRIVATE)
	private PreparedStatement prepCols;

	@Getter(value = AccessLevel.PRIVATE)
	@Setter(value = AccessLevel.PRIVATE)
	private PreparedStatement prepRowkey;

	public long getBeginWhen() {
		if (this.beginWhen != 0) {
			if (task == null || StringUtil.isNull(task.getBeginTime()))
				this.beginWhen = -1l;
			else {
				try {
					long retlong = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().parse(task.getBeginTime()).getTime()
							/ 1000;
					this.beginWhen = retlong;
				} catch (ParseException e) {
					this.beginWhen = -1l;
				}
			}
		}
		return this.beginWhen;
	}

	/***
	 * 找到表的匹配规则
	 * 
	 * @param db
	 * @param tb
	 * @return
	 */
	public Rule findRule(String db, String tb) {
		for (Rule rule : this.task.getRuleList()) {
			if (!"^*$".equals(rule.getDbPattern())) {
				boolean retdb = StrPattern.checkStrFormat(rule.getDbPattern(), db);
				if (!retdb) {
					continue;
				}
			}
			if (!"^*$".equals(rule.getTbPattern())) {
				boolean rettb = StrPattern.checkStrFormat(rule.getTbPattern(), tb);
				if (!rettb) {
					continue;
				}
			}
			return rule;
		}
		return null;
	}

	/***
	 * 验证col是否正确，如果map没有代表是新的，需要读数据库获得
	 * 
	 * @param db
	 * @param tb
	 * @param time
	 * @return
	 */
	public ColHis ValidKey(String db, String tb, long time) {
		String key = String.format("%s|%s", db, tb).toLowerCase();
		if (colsMap.containsKey(key)) {
			SortedSet<ColHis> hiss = colsMap.get(key);
			for (ColHis colHis : hiss) {
				if (colHis.getTime() < time) {
					return colHis;
				}
			}
			throw new RuntimeException("没有可用的col信息。");//由于第1条时间设置为-1,所以它一般不会被执行
		} else {
			return addCols(db, tb, getBeginWhen());
		}
	}

	public String buildInstalName() {
		String instName = Main.context.getTask().getDbinst();
		if (StringUtil.isNull(instName) || "no".equals(instName)) {
			instName = String.format("%s:%s", Main.context.getTask().getIp(), Main.context.getTask().getPort());
		}
		return instName;
	}

	/***
	 * 日志的时间，秒
	 * 
	 * @param db
	 * @param tb
	 * @param time
	 * @return
	 */
	public ColHis addCols(String db, String tb, long time) {
		String key = String.format("%s|%s", db, tb).toLowerCase();
		java.sql.Connection conn = null;
		try {
			String url = String.format("jdbc:mysql://%s:%s?autoReconnect=true&useUnicode=true&characterEncoding=utf-8",
					this.task.getIp(), this.task.getPort());
			conn = JdbcConnection.getConnection("com.mysql.jdbc.Driver", url, this.task.getUser(), this.task.getPwd());
			if (prepCols == null || prepCols.isClosed()) {//
				prepCols = conn.prepareStatement(
						"select   column_name,data_type   from  information_schema.columns  where  table_schema=? and table_name=?");
			}
			List<String> ret = new ArrayList<>();
			List<String> retType = new ArrayList<>();
			JdbcAssit.setPreParam(prepCols, db, tb);
			ResultSet rs = prepCols.executeQuery();
			while (rs.next()) {
				ret.add(rs.getString(1));
				retType.add(rs.getString(2));
			}
			rs.close();
			if (CollectionUtils.isEmpty(ret)) {
				log.error("db:{},td:{},user:{} 没有s查询到列名，请检查用户是否有此权限", db, tb, this.task.getUser());
				LoggerUtil.exit(JvmStatus.s15);
			}
			if (YesOrNo.yes == this.task.getRds()) {// 是rds
				if (prepRowkey == null || prepRowkey.isClosed()) {
					prepRowkey = conn.prepareStatement(
							"SELECT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING (constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema=? AND t.table_name=?");

				}
				JdbcAssit.setPreParam(prepRowkey, db, tb);
				ResultSet rs2 = prepRowkey.executeQuery();
				if (!rs2.next()) {
					ret.add("_rowkey_");
					retType.add("varchar");
				}
				rs2.close();
			}
			ColHis retobj = new ColHis();
			retobj.setTime(time);
			retobj.setDb(db);
			retobj.setTb(tb);
			retobj.setCols(ret.toArray(new String[ret.size()]));
			retobj.setColTypes(retType.toArray(new String[retType.size()]));
			SortedSet<ColHis> templist = colsMap.containsKey(key) ? colsMap.get(key) : new TreeSet<ColHis>();
			if (!templist.contains(retobj)) {
				if(templist.size()==0) {
					retobj.setTime(-1);//20190311 第一条记录设置为-1，全量匹配,防止较早的binlog被延时发送,但有col不匹配的风险
				}
				templist.add(retobj);
			}
			colsMap.put(key, templist);
			ZkUtil.updateCols(buildInstalName(), key, templist);
			return retobj;
		} catch (Exception e) {
			log.error("获取cols错误", e);
			throw new RuntimeException("获取cols错误");
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error("关闭连接失败", e);
				}
			}
		}
	}

}
