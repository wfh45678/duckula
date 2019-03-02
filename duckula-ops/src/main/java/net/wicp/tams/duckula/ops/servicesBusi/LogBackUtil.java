package net.wicp.tams.duckula.ops.servicesBusi;

import java.nio.charset.Charset;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class LogBackUtil {
	private static final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

	/***
	 * 得到根logger
	 * 
	 * @return
	 */
	public static Logger getRoot() {
		return context.getLogger(Logger.ROOT_LOGGER_NAME);
	}

	public static RollingFileAppender<ILoggingEvent> newFileAppender(String filePath, PatternLayoutEncoder filePattern,
			TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy) {

		final RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<ILoggingEvent>();
		fileAppender.setContext(context);

		fileAppender.setFile(filePath);

		rollingPolicy.setParent(fileAppender);
		fileAppender.setRollingPolicy(rollingPolicy);
		rollingPolicy.start();

		fileAppender.setEncoder(filePattern);
		filePattern.start();

		fileAppender.start();
		return fileAppender;
	}

	/***
	 * 创建文件appender
	 * 
	 * @param filePath
	 *            日志文件存放路径（包含文件名） eg:/aa/bb.log
	 * @param encoderPattern
	 *            日志输出格式 eg:%msg%n
	 * @param fileNamePattern
	 *            日志文件名循环模式 eg:/aa/bbb-%d{yyyy-MM-dd}.log
	 * @param maxHistory
	 *            日志文件最大 eg:30
	 * @return
	 */
	public static RollingFileAppender<ILoggingEvent> newFileAppender(String filePath, String encoderPattern,
			String fileNamePattern, int maxHistory) {
		final PatternLayoutEncoder filePattern = LogBackUtil.buildEncoder(encoderPattern);
		final TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = LogBackUtil.buildRollingPolicy(fileNamePattern,
				maxHistory);

		RollingFileAppender<ILoggingEvent> fileAppender = LogBackUtil.newFileAppender(filePath, filePattern,
				rollingPolicy);
		return fileAppender;
	}

	public static PatternLayoutEncoder buildEncoder(String pattern) {
		final PatternLayoutEncoder filePattern = new PatternLayoutEncoder();
		filePattern.setContext(context);
		filePattern.setCharset(Charset.forName("UTF-8"));
		filePattern.setPattern(pattern);
		return filePattern;
	}

	public static TimeBasedRollingPolicy<ILoggingEvent> buildRollingPolicy(String fileNamePattern, int maxHistory) {
		TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<ILoggingEvent>();
		rollingPolicy.setContext(context);
		rollingPolicy.setFileNamePattern(fileNamePattern);
		rollingPolicy.setMaxHistory(maxHistory);
		return rollingPolicy;
	}

	public static void logDebugTime(org.slf4j.Logger loginput, String info, long beginTime) {
		if (loginput == null) {
			loginput = log;
		}
		if (loginput.isDebugEnabled()) {
			loginput.debug(String.format("time:info:[%s] begin:[%s] used:[%s] ", info, beginTime,
					System.currentTimeMillis() - beginTime));
		}
	}

	public static void logDebugTime(String info, long beginTime) {
		logDebugTime(null, info, beginTime);
	}

}
