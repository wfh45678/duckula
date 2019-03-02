package net.wicp.tams.duckula.task.test.parser;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import net.wicp.tams.common.binlog.parser.DirectLogFetcher;
import net.wicp.tams.common.binlog.parser.LogContext;
import net.wicp.tams.common.binlog.parser.LogDecoder;
import net.wicp.tams.common.binlog.parser.LogEvent;
import net.wicp.tams.common.binlog.parser.event.DeleteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.QueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.RotateLogEvent;
import net.wicp.tams.common.binlog.parser.event.RowsQueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.UpdateRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.WriteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.XidLogEvent;
import net.wicp.tams.common.binlog.parser.event.mariadb.AnnotateRowsEvent;

import org.junit.Assert;
import org.junit.Test;

public class DirectLogFetcherTest extends BaseLogFetcherTest {

	@Test
	public void testSimple() {
		DirectLogFetcher fecther = new DirectLogFetcher();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306", "root", "111111");
			Statement statement = connection.createStatement();
			statement.execute("SET @master_binlog_checksum='@@global.binlog_checksum'");
			statement.execute("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");

			fecther.open(connection, "mysql_bin.000001", 4L, 2);

			LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
			LogContext context = new LogContext();
			while (fecther.fetch()) {
				LogEvent event = null;
				event = decoder.decode(fecther, context);

				if (event == null) {
					continue;
					// throw new RuntimeException("parse failed");
				}

				int eventType = event.getHeader().getType();
				switch (eventType) {
				case LogEvent.ROTATE_EVENT:
					binlogFileName = ((RotateLogEvent) event).getFilename();
					break;
				case LogEvent.WRITE_ROWS_EVENT_V1:
				case LogEvent.WRITE_ROWS_EVENT:
					parseRowsEvent((WriteRowsLogEvent) event);
					break;
				case LogEvent.UPDATE_ROWS_EVENT_V1:
				case LogEvent.UPDATE_ROWS_EVENT:
					parseRowsEvent((UpdateRowsLogEvent) event);
					break;
				case LogEvent.DELETE_ROWS_EVENT_V1:
				case LogEvent.DELETE_ROWS_EVENT:
					parseRowsEvent((DeleteRowsLogEvent) event);
					break;
				case LogEvent.QUERY_EVENT:
					parseQueryEvent((QueryLogEvent) event);
					break;
				case LogEvent.ROWS_QUERY_LOG_EVENT:
					parseRowsQueryEvent((RowsQueryLogEvent) event);
					break;
				case LogEvent.ANNOTATE_ROWS_EVENT:
					parseAnnotateRowsEvent((AnnotateRowsEvent) event);
					break;
				case LogEvent.XID_EVENT:
					parseXidEvent((XidLogEvent) event);
					break;
				default:
					break;
				}
			}
		} catch (SQLException e) {//得到联接错误
			e.printStackTrace();
			Assert.fail(e.getMessage());

		} catch (IOException e) {//拉取日志文件错误
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {//其它错误
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				fecther.close();
			} catch (IOException e) {
				Assert.fail(e.getMessage());
			}
		}

	}
}
