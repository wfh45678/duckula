package net.wicp.tams.duckula.ops.servicesBusi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.beans.Host;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectExceptionRuntime;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.common.os.pool.SSHPoolByMap;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Slf4j
public abstract class DuckulaUtils {
	private static Set<Long> hasLoggers = new HashSet<>();

	public static SSHPoolByMap sSHPoolByMap = null;
	static {
		GenericObjectPoolConfig<SSHConnection> config = new GenericObjectPoolConfig<SSHConnection>();
		config.setMaxTotal(4);
		List<Host> conf = new ArrayList<>();
		sSHPoolByMap = new SSHPoolByMap(conf, config);
	}

	public static SSHConnection getConn(Host host) {
		try {
			SSHConnection conn = sSHPoolByMap.borrowObject(host);
			return conn;
		} catch (Exception e) {
			throw new ProjectExceptionRuntime(ExceptAll.ssh_pool_conn, "连接出错");
		}
	}

	public static List<String> findTaskIdByNamespace(String namespace) {
		if (StringUtil.isNull(namespace)) {
			return new ArrayList<String>();
		}
		List<Task> allTasks = ZkUtil.findAllObjs(ZkPath.tasks, Task.class);
		CollectionUtils.filter(allTasks, new Predicate() {

			@Override
			public boolean evaluate(Object object) {
				if (TaskPattern.isNeedServer()||"all".equalsIgnoreCase(namespace)) {
					return true;
				}
				Task task = (Task) object;
				return namespace.equalsIgnoreCase(task.getNamespace());
			}
		});
		List<String> colFromObj = (List<String>) CollectionUtil.getColFromObj(allTasks, "id");
		return colFromObj;
	}

	public static void returnConn(Host host, SSHConnection conn) {
		try {
			sSHPoolByMap.returnObject(host, conn);
		} catch (Exception e) {
			log.error(String.format("回收连接错误,ip:%s,port:%s", host.getHostIp(), host.getPort()), e);
		}
	}

	public static void returnConn(Server server, SSHConnection conn) {
		returnConn(Host.builder().hostIp(server.getIp()).port(server.getServerPort()).build(), conn);
	}

	public static SSHConnection getConn(Server server) {
		SSHConnection conn = DuckulaUtils
				.getConn(Host.builder().hostIp(server.getIp()).port(server.getServerPort()).build());
		return conn;
	}

	/***
	 * 跟据masterId得到对应的logger
	 * 
	 * @param masterId
	 * @return
	 */
	public static Logger getPosLog(long masterId) {
		Logger logger = null;
		if (!hasLoggers.contains(masterId)) {
			String logRoot = System.getenv("DUCKULA_DATA") + "/logs/pos";
			final String filepath = "%s/%s/pos%s.log";
			String maxsize = StringUtil.hasNull(Conf.get("duckula.ops.pos.maxsize"), "7");
			RollingFileAppender<ILoggingEvent> fileAppender = LogBackUtil.newFileAppender(
					String.format(filepath, logRoot, masterId, ""), "%msg%n",
					String.format(filepath, logRoot, masterId, "-%d{yyyy-MM-dd}"), Integer.parseInt(maxsize));
			logger = (Logger) LoggerFactory.getLogger(String.valueOf(masterId));// za_sleuth
			logger.addAppender(fileAppender);
			logger.setLevel(Level.INFO);
			hasLoggers.add(masterId);
		} else {
			logger = (Logger) LoggerFactory.getLogger(String.valueOf(masterId));// za_sleuth
		}
		return logger;
	}

	/***
	 * 得到当前的主的的位点
	 * 
	 * @param masterId
	 * @return
	 */
	public static String openPosLog(long masterId, Date queryDate) {
		String logRoot = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/logs/pos");
		String path = "";
		if (queryDate == null || DateUtils.isSameDay(queryDate, new Date())) {
			path = String.format("%s/%s/pos.log", logRoot, masterId);
		} else {
			path = String.format("%s/%s/pos-%s.log", logRoot, masterId,
					DateFormatCase.YYYY_MM_DD.getInstanc().format(queryDate));
		}
		return path;
	}

	public static List<Pos> readPosLogReverse(long masterId, Date queryDate, int maxNum) {
		Set<Pos> retpos = new HashSet<>();
		String path = openPosLog(masterId, queryDate);
		List<Pos> ret = new ArrayList<>();
		List<String> retlist = IOUtil.readFileReverse(path, maxNum);
		if (CollectionUtils.isEmpty(retlist)) {
			return ret;
		}
		for (String str : retlist) {
			if (StringUtil.isNull(str)) {
				continue;
			}
			retpos.add(JSONObject.parseObject(str, Pos.class));
		}
		ret.addAll(retpos);
		Collections.sort(ret);
		return ret;
	}

	public static List<Pos> readPosLogReverse(long masterId, int maxNum) {
		return readPosLogReverse(masterId, null, maxNum);
	}

	///////
	public static Map<String, String[]> packHosts(Rule... rules) {
		Map<String, String[]> retmap = new HashMap<>();
		if (ArrayUtils.isEmpty(rules)) {
			return retmap;
		}
		for (Rule rule : rules) {
			if (MapUtils.isEmpty(rule.getItems())) {
				continue;
			}
			if (StringUtil.isNotNull(rule.getItems().get(RuleItem.middleware))) {// es中间件
				Map<String, String[]> hostMap = MiddlewareType.es.getHostMap(rule.getItems().get(RuleItem.middleware));
				retmap.putAll(hostMap);
			}
		}
		return retmap;
	}

	/*
	 * public static JSONObject buildClient(Command command) { JSONObject retobj =
	 * new JSONObject(); JSONObject clientobj = new JSONObject();
	 * clientobj.put("senderSystem", "duckula"); clientobj.put("senderApplication",
	 * "ops"); clientobj.put("version", "1.0"); clientobj.put("senderChannel",
	 * "H5"); clientobj.put("msgId", "1493708268936");
	 * clientobj.put("requestCommand", command.name()); retobj.put("ControlInfo",
	 * clientobj); return retobj; }
	 */

}
