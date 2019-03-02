package net.wicp.tams.duckula.task.conf;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;

public abstract class AbsTaskConf implements ITaskConf {
	protected String taskId;

	@Override
	public final void init(String taskId) {
		this.taskId = taskId;
		initSub();
	}

	public Pos getMastStatus(Task task) {
		String url;
		if (StringUtil.isNull(task.getDefaultDb())) {
			url = String.format("jdbc:mysql://%s:%s?autoReconnect=true&useUnicode=true&characterEncoding=utf-8",
					task.getIp(), task.getPort());
		} else {
			url = String.format("jdbc:mysql://%s:%s/%s?autoReconnect=true&useUnicode=true&characterEncoding=utf-8",
					task.getIp(), task.getPort(), task.getDefaultDb());
		}
		java.sql.Connection conn = JdbcConnection.getConnection("com.mysql.jdbc.Driver", url, task.getUser(), task.getPwd());
		ResultSet rs = JdbcAssit.querySql(conn, "show master status");
		try {
			if (rs.next()) {
				String filename = rs.getString(1);
				long pos = rs.getLong(2);
				Pos ret = new Pos();
				ret.setFileName(filename);
				ret.setPos(pos);
				if (rs.getMetaData().getColumnCount() >= 5) {
					String gtidStr = rs.getString(5);
					ret.setGtids(gtidStr.replace("/n", ""));
				}
				rs = JdbcAssit.querySql(conn, "show variables like 'server_id'");
				if (rs.next()) {
					long masterServerId = rs.getLong(2);
					ret.setMasterServerId(masterServerId);
				}
				return ret;
			}
			throw new RuntimeException("没有得到mastStatus,服务器不支持binlog");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				rs.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public abstract void initSub();
}
