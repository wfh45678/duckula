package net.wicp.tams.duckula.task.test.parser;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import net.wicp.tams.common.binlog.parser.FileLogFetcher;
import net.wicp.tams.common.binlog.parser.LogContext;
import net.wicp.tams.common.binlog.parser.LogDecoder;
import net.wicp.tams.common.binlog.parser.LogEvent;
import net.wicp.tams.common.binlog.parser.LogPosition;
import net.wicp.tams.common.binlog.parser.event.DeleteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.QueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.RotateLogEvent;
import net.wicp.tams.common.binlog.parser.event.RowsQueryLogEvent;
import net.wicp.tams.common.binlog.parser.event.UpdateRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.WriteRowsLogEvent;
import net.wicp.tams.common.binlog.parser.event.XidLogEvent;
import net.wicp.tams.common.binlog.parser.event.mariadb.AnnotateRowsEvent;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileLogFetcherTest extends BaseLogFetcherTest {

    private String directory;

    @Before
    public void setUp() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        directory = new File(dummyFile.getParent() + "/binlog").getPath();
    }

    @Test
    public void testSimple() {
        FileLogFetcher fetcher = new FileLogFetcher(1024 * 16);
        try {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();

            File current = new File(directory, "mysql-bin.000001");
            fetcher.open(current, 4L);
            context.setLogPosition(new LogPosition(current.getName()));

            while (fetcher.fetch()) {
                LogEvent event = null;
                event = decoder.decode(fetcher, context);
                if (event != null) {
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
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                fetcher.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }
}
