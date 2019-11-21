package net.wicp.tams.duckula.ops.servicesBusi;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import common.kubernetes.tiller.TillerClient;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.beans.Host;
import net.wicp.tams.common.constant.EPlatform;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.os.bean.DockContainer;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.common.os.tools.DockerAssit;
import net.wicp.tams.common.thread.ThreadPool;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.beans.PosShow;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.plugin.beans.Rule;

@Slf4j
public class DuckulaAssitImpl implements IDuckulaAssit {

	@Override
	public List<Server> findAllServers() {
		List<Server> servers = new ArrayList<>();
		List<String> allServers = ZkUtil.findSubNodes(ZkPath.servers);
		for (String serverName : allServers) {
			Server server = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.servers.getPath(serverName)),
					Server.class);
			servers.add(server);
		}
		return servers;
	}

	@Override
	public void reStartTask(CommandType commandType, String childrenId, String... removeIps) {
		YesOrNo needRun = YesOrNo.no;
		ZkPath zkPath = null;
		switch (commandType) {
		case task:
			Task task = ZkUtil.buidlTask(childrenId);
			needRun = task == null ? YesOrNo.no : task.getRun();
			zkPath = ZkPath.tasks;
			break;
		case consumer:
			Consumer consumer = ZkUtil.buidlConsumer(childrenId);
			needRun = consumer == null ? YesOrNo.no : consumer.getRun();
			zkPath = ZkPath.consumers;
			break;
		default:
			break;
		}
		if (needRun == YesOrNo.yes && zkPath != null) {// 需要启动
			List<String> locks = ZkUtil.lockIps(zkPath, childrenId);
			if (CollectionUtils.isEmpty(locks)) {// 没有启动
				log.info("类型:{} taskId:[{}]被下线.", commandType.name(), childrenId);
				try {
					Server server = this.selServer(removeIps);// 要剔除的ＩＰ
					if (server == null) {
						log.error("没有可用的服务器运行任务");
					} else {
						Result result = this.startTask(commandType, childrenId, server, true);
						log.info("类型:{} taskId:[{}],task试着重启结果{}.", commandType.name(), childrenId,
								result.getMessage());
					}
				} catch (Throwable e) {
					log.error("运行任务失败", e);
				}

			}
		}
	}

	@Override
	public List<DbInstance> findAllDbInstances() {
		List<DbInstance> dbs = new ArrayList<>();
		List<String> allDbNames = ZkUtil.findSubNodes(ZkPath.dbinsts);
		for (String dbName : allDbNames) {
			DbInstance db = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(dbName)),
					DbInstance.class);
			dbs.add(db);
		}
		return dbs;
	}

	@Override
	public List<Task> findAllTasks() {
		List<Task> tasks = new ArrayList<>();
		List<String> allTaskNames = ZkUtil.findSubNodes(ZkPath.tasks);
		for (String taskNames : allTaskNames) {
			Task tk = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.tasks.getPath(taskNames)),
					Task.class);
			tasks.add(tk);
		}
		return tasks;
	}

	@Override
	public List<PosShow> findAllPosForTasks() {
		List<PosShow> poslist = new ArrayList<>();
		List<String> allTaskNames = ZkUtil.findSubNodes(ZkPath.tasks);
		for (String taskName : allTaskNames) {
			PosShow pos = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.pos.getPath(taskName)),
					PosShow.class);
			if (pos == null) {
				pos = new PosShow();
			}
			pos.setId(taskName);
			poslist.add(pos);
		}
		return poslist;
	}

	@Override
	public Map<String, Map<ZkPath, List<String>>> serverRunTaskDetail(List<Server> servers) {
		Map<String, Map<ZkPath, List<String>>> retMap = new HashMap<>();
		for (Server server : servers) {
			Map<ZkPath, List<String>> tempmap = new HashMap<>();
			tempmap.put(ZkPath.tasks, new ArrayList<>());
			tempmap.put(ZkPath.consumers, new ArrayList<>());
			if ("localhost".equals(server.getIp())) {
				retMap.put(server.getIp(), tempmap);
				continue;
			}
			// task显示
			List<String> taskIds = ZkClient.getInst().getChildren(ZkPath.tasks.getRoot());
			CollectionUtils.filter(taskIds, new Predicate() {
				@Override
				public boolean evaluate(Object object) {
					return StringUtil.isNotNull(object);
				}
			});
			for (String taskId : taskIds) {
				List<String> locksServers = lockToServer(servers, ZkPath.tasks, taskId);
				if (locksServers.contains(server.getIp())) {
					tempmap.get(ZkPath.tasks).add(taskId);
				}
			}
			// consumer显示
			List<String> consumerIds = ZkClient.getInst().getChildren(ZkPath.consumers.getRoot());
			CollectionUtils.filter(consumerIds, new Predicate() {
				@Override
				public boolean evaluate(Object object) {
					return StringUtil.isNotNull(object);
				}
			});
			for (String consumerId : consumerIds) {
				List<String> locksServers = lockToServer(servers, ZkPath.consumers, consumerId);
				if (locksServers.contains(server.getIp())) {
					tempmap.get(ZkPath.consumers).add(consumerId);
				}
			}

			retMap.put(server.getIp(), tempmap);
		}
		return retMap;
	}

	@Override
	public Map<String, Integer> serverRunTaskNum(List<Server> servers) {
		Map<String, Integer> retMap = new HashMap<>();
		for (Server server : servers) {
			retMap.put(server.getIp(), 0);
		}
		List<String> taskIds = ZkClient.getInst().getChildren(ZkPath.tasks.getRoot());
		CollectionUtils.filter(taskIds, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return StringUtil.isNotNull(object);
			}
		});
		for (String taskId : taskIds) {
			// if (task.getRun() == YesOrNo.yes) { 有可能手工起起来了，但没有设置为yes
			// List<String> locks = ZkUtil.lockIps(task.getId());
			// }
			List<String> locksServers = lockToServer(servers, ZkPath.tasks, taskId);
			if (CollectionUtils.isNotEmpty(locksServers)) {
				for (Server server : servers) {
					if (locksServers.contains(server.getIp())) {
						int tempNum = retMap.get(server.getIp()).intValue();
						retMap.put(server.getIp(), ++tempNum);
					}
				}
			}
		}

		List<String> consumerIds = ZkClient.getInst().getChildren(ZkPath.consumers.getRoot());
		CollectionUtils.filter(consumerIds, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return StringUtil.isNotNull(object);
			}
		});
		for (String consumerId : consumerIds) {
			List<String> locksServers = lockToServer(servers, ZkPath.consumers, consumerId);
			if (CollectionUtils.isNotEmpty(locksServers)) {
				for (Server server : servers) {
					if (locksServers.contains(server.getIp())) {
						int tempNum = retMap.get(server.getIp()).intValue();
						retMap.put(server.getIp(), ++tempNum);
					}
				}
			}
		}
		return retMap;
	}

	public List<String> lockToServer(List<Server> findAllServers, ZkPath zkPath, String taskId) {
		List<String> serverids = new ArrayList<>();// 转成IP的值
		if (zkPath == null || StringUtil.isNull(taskId)) {
			return serverids;
		}
		List<String> locks = ZkUtil.lockIps(zkPath, taskId);// 分布式锁里的值
		for (String lock : locks) {
			for (Server server : findAllServers) {
				if (lock.equals(server.getLockIp())) {
					serverids.add(server.getIp());
				}
			}
		}
		return serverids;
	}

	@Override
	public Server selServer(final String... removeIps) {
		List<Server> servers = findAllServers();
		CollectionUtils.filter(servers, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Server server = (Server) object;
				return !"localhost".equals(server.getIp())
						&& (removeIps == null || !ArrayUtils.contains(removeIps, server.getLockIp()));
			}
		});
		Server.packageResources(servers);// 需要查找资源
		Collections.sort(servers);
		if (CollectionUtils.isEmpty(servers)) {
			return null;
		} else {
			return servers.get(0);
		}
	}

	// 默认Conf.get("common.kubernetes.apiserver.namespace.default")
	@Override
	public Result startTaskForK8s(CommandType commandType, String taskId, boolean isAuto) {
		Task buidlTask = QueryTask(commandType, taskId);
		boolean standalone = Conf.getBoolean("duckula.ops.starttask.standalone");
		if (buidlTask == null) {
			return Result.getError("不支持的的类型或taskId不正确");
		}
		String chartsDirPath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/k8s/duckula_task");
		List<Object> userList = new ArrayList<>();
		userList.add("imageTaskTag");
		userList.add(buidlTask.getImageVersion());
		userList.add("cmd");
		userList.add(commandType.getK8scmd());
		userList.add("schedule");
		userList.add(commandType.getK8sSchedule());
		userList.add("env.taskid");
		userList.add(taskId);
		userList.add("env.zk");
		userList.add(Conf.get("common.others.zookeeper.constr"));
		userList.add("env.rootpath");// 设置zk的root目录
		userList.add(Conf.get("duckula.zk.rootpath"));

		if (commandType == CommandType.dump) {// 默认是“nojob”需要被覆盖
			Dump buidlDump = ZkUtil.buidlDump(taskId);// 需要dump来判断设置执行时间
			if (StringUtil.isNull(buidlDump.getSchedule())) {// 是job
				userList.add("schedule");
				userList.add(buidlDump.getSchedule());
			} else {// 是Schedule
				userList.add("schedule");
				userList.add("now");
			}
		}

		log.info("-----------taskId:{},zk:{},rootpath:{}-----------", taskId,
				Conf.get("common.others.zookeeper.constr"), Conf.get("duckula.zk.rootpath"));
		String idfull = commandType.getK8sId(taskId);
		String name = idfull.length() <= 63 ? idfull : idfull.substring(0, 63);
		// 跨名称空间部署
		String defaultNamespace = Conf.get("common.kubernetes.apiserver.namespace.default");
		if (!defaultNamespace.equals(buidlTask.getNamespace())) {
			userList.add("persistence.enabled");
			userList.add(false);
			String poststr = Conf.get("common.kubernetes.apiserver.namespace.valuepost");
			Object[] userConfig = userList.toArray(new Object[userList.size()]);
			Result installDirChart = TillerClient.getInst().installDirChart(name, buidlTask.getNamespace(),
					chartsDirPath, String.format("values-%s.yaml", poststr), userConfig);
			if (!installDirChart.isSuc()) {
				log.error("在k8s上启动Task[{}]出错:{}", taskId, installDirChart.getMessage());
			} else {
				if (isAuto) {
					setAuto(commandType, taskId, YesOrNo.yes);
				}
			}
			return installDirChart;
		}

		// 设置hosts
		Map<String, String[]> hosts = buidlTask.getMiddlewareType().getHostMap(buidlTask.getMiddlewareInst());
		if (MapUtils.isNotEmpty(hosts)) {
			JSONArray hostAry = new JSONArray();
			for (String ip : hosts.keySet()) {
				JSONObject temp = new JSONObject();
				temp.put("ip", ip);
				JSONArray ary = new JSONArray();
				for (String host : hosts.get(ip)) {
					ary.add(host);
				}
				temp.put("host", ary);
				hostAry.add(temp);
			}
			userList.add("hosts");
			userList.add(hostAry);
		}

		userList.add("persistence.enabled");
		userList.add(!standalone);
		if (!standalone) {// 非独立模式
			// eg:"default/mypvc"
			String claimName = Conf.get("duckula.ops.starttask.claimname");
			if (StringUtil.isNull(claimName)) {
				return Result.getError(
						"task的非独立模式，需要设置claimName环境变量，找到OPS使用的PVC，把它设置为claimName环境变量或是修改ops的配置：duckula.ops.starttask.claimname");
			}
			userList.add("persistence.existingClaim");
			userList.add(claimName);
		} else {

		}
		Object[] userConfig = userList.toArray(new Object[userList.size()]);

		Result result = TillerClient.getInst().installDirChart(name, buidlTask.getNamespace(), chartsDirPath,
				userConfig);
		if (!result.isSuc()) {
			log.error("在k8s上启动Task[{}]出错:{}", taskId, result.getMessage());
		} else {
			if (isAuto) {
				setAuto(commandType, taskId, YesOrNo.yes);
			}
		}
		return result;
	}

	private Task QueryTask(CommandType commandType, String taskId) {
		Task buidlTask = null;
		switch (commandType) {
		case task:
			buidlTask = ZkUtil.buidlTask(taskId);
			break;
		case consumer:
			Consumer buidlConsumer = ZkUtil.buidlConsumer(taskId);
			buidlTask = ZkUtil.buidlTask(buidlConsumer.getTaskOnlineId());
			break;
		case dump:
			Dump buidlDump = ZkUtil.buidlDump(taskId);
			buidlTask = ZkUtil.buidlTask(buidlDump.getTaskOnlineId());
			break;
		default:
			break;
		}
		return buidlTask;
	}

	// isAuto
	@Override
	public Result stopTaskForK8s(CommandType commandType, String taskId, boolean isAuto) {
		if (true) {
			setAuto(commandType, taskId, YesOrNo.no);
		}
		String idfull = commandType.getK8sId(taskId);
		Result deleteChart = TillerClient.getInst().deleteChart(idfull);

		if (!deleteChart.isSuc()) {
			log.error("在k8s上停止Task[{}]出错:{}", taskId, deleteChart.getMessage());
		} else {
			if (isAuto) {
				setAuto(commandType, taskId, YesOrNo.yes);
				waitUnLock(commandType, taskId, 120000);
			}
		}
		return deleteChart;
	}

	@Override
	public Result startTask(CommandType commandType, String taskId, Server server, boolean isAuto) {
		if (server == null) {
			return Result.getError("没有可用服务");
		}
		// 调试用
		if (server.getOs() != null && server.getOs() == EPlatform.Windows) {
			return startWindows(commandType, taskId);
		}
		int jmxPort = StringUtil.buildPort(commandType.name() + "_" + taskId);
		SSHConnection conn = DuckulaUtils
				.getConn(Host.builder().hostIp(server.getIp()).port(server.getServerPort()).build());
		Result result = null;
		if (server.getUseDocker() == YesOrNo.yes) {
			// ServerCommon serverCommon = server.findServerCommon();
			JSONArray packHosts = packHosts(commandType, taskId);
			String hostsstr = "";
			if (packHosts != null && packHosts.size() > 0) {
				StringBuffer buff = new StringBuffer();
				for (int i = 0; i < packHosts.size(); i++) {
					JSONObject jsonObject = packHosts.getJSONObject(i);
					String ip = jsonObject.getString("ip");
					JSONArray hostnames = jsonObject.getJSONArray("host");
					for (int j = 0; j < hostnames.size(); j++) {
						String hostname = hostnames.getString(j);
						buff.append(String.format("--add-host %s:%s ", hostname, ip));
					}
				}
				hostsstr = buff.toString();
			}
			String startCmd = DockerAssit.run(
					String.format("%s:%s", Conf.get("duckula.task.image.name"), Conf.get("duckula.task.image.tag")),
					String.format("%s %s %s", commandType.getDockerCmd(EPlatform.Linux), taskId,
							ConfUtil.defaulJmxPort),
					new String[] { "-d", "-e \"zk=" + Conf.get("common.others.zookeeper.constr") + "\"",
							"-e  \"ip=" + server.getLockIp() + "\"", "-p " + jmxPort + ":" + ConfUtil.defaulJmxPort,
							"-v /data/duckula-data:" + ConfUtil.getDatadir(true), hostsstr });// serverCommon.packAddHostParams()
			result = conn.executeCommand(startCmd, 600000);// 最大允许执行10分钟，用于拉镜像
		} else {
			String startCmd = IOUtil.mergeFolderAndFilePath("sh ", Conf.get("duckula.ops.homedir"),
					commandType.getBatchFile(EPlatform.Linux));
			result = conn.executeCommand(String.format("%s %s %s", startCmd, taskId, jmxPort));
		}
		// DuckulaUtils.returnConn(server, conn);
		if (!result.isSuc()) {
			log.error("在服务器:[{}]上启动Task[{}]出错:{}", server.getIp(), taskId, result.getMessage());
		} else {
			if (!result.getMessage().contains("serverice end") && server.getUseDocker() == YesOrNo.no) {
				return Result.getError(result.getMessage());
			}
			if (isAuto) {
				setAuto(commandType, taskId, YesOrNo.yes);
			}
		}
		return result;
	}

	public Result startWindows(CommandType commandType, String taskId) {
		String cmd = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_HOME"),
				commandType.getBatchFile(EPlatform.Windows));
		int jmxPort = StringUtil.buildPort(taskId);
		String startCmd = String.format("cmd /c start %s %s %s", cmd, taskId, jmxPort);
		try {
			final Process ps = Runtime.getRuntime().exec(startCmd);
			ps.waitFor(3, TimeUnit.SECONDS);
			// String instr = IOUtil.slurp(ps.getInputStream(),
			// Conf.getSystemEncode());//会无限等待
			Future<String> query = (Future<String>) ThreadPool.getDefaultPool().submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					String errstr = IOUtil.slurp(ps.getErrorStream(), Conf.getSystemEncode());
					return errstr;
				}
			});
			try {
				String retstr = query.get(10, TimeUnit.SECONDS);
				log.error("启动失败,原因:[{}]", retstr);
				return Result.getError(retstr);
			} catch (Exception e) {
				log.info("超时获得错误流，意味着命令没问题");
				return Result.getSuc();
			}

		} catch (IOException ioe) {
			log.error("IO异常，文件有误", ioe);
			return Result.getError(ioe.getMessage());
		} catch (InterruptedException e) {
			log.error("中断异常", e);
			return Result.getError(e.getMessage());
		}
	}

	/**
	 * eg:
	 * [{"ip":"127.0.0.1","host":["zk-kafka-04"]},{"ip":"127.0.0.2","host":["zk-kafka-03"]}]
	 * 
	 * @param commandType
	 * @param buidlTask
	 * @return
	 */
	private JSONArray packHosts(CommandType commandType, String taskId) {
		Task buidlTask = QueryTask(commandType, taskId);
		if (buidlTask == null) {
			return null;
		}
		Map<String, String[]> hosts = buidlTask.getMiddlewareType().getHostMap(buidlTask.getMiddlewareInst());
		if (commandType == CommandType.consumer) {// es可能有要求
			Consumer buidlConsumer = ZkUtil.buidlConsumer(taskId);
			Map<String, String[]> packHosts = DuckulaUtils
					.packHosts(buidlConsumer.getRuleList().toArray(new Rule[buidlConsumer.getRuleList().size()]));
			hosts.putAll(packHosts);
		}
		if (commandType == CommandType.dump) {
			Dump buidlDump = ZkUtil.buidlDump(taskId);
			Map<String, String[]> hostMap = MiddlewareType.es.getHostMap(buidlDump.getCluster());
			hosts.putAll(hostMap);
		}
		if (MapUtils.isNotEmpty(hosts)) {
			JSONArray hostAry = new JSONArray();
			for (String ip : hosts.keySet()) {
				JSONObject temp = new JSONObject();
				temp.put("ip", ip);
				JSONArray ary = new JSONArray();
				for (String host : hosts.get(ip)) {
					ary.add(host);
				}
				temp.put("host", ary);
				hostAry.add(temp);
			}
			return hostAry;
		}
		return null;
	}

	@Override
	public Result stopTask(CommandType commandType, String taskId, Server server, boolean isAuto) {
		if (!isAuto) {
			setAuto(commandType, taskId, YesOrNo.no);
		}
		SSHConnection conn = DuckulaUtils.getConn(server);
		Result result = null;
		if (server.getUseDocker() == YesOrNo.yes) {
			List<DockContainer> dockerps = conn.dockerps(taskId, Conf.get("duckula.task.image.tag"));
			if (CollectionUtils.isEmpty(dockerps)) {
				String str = String.format("在服务器:[%s]上停止Task[%s]出错:不存在此task", server.getIp(), taskId);
				log.error(str);
				return Result.getError(str);
			}
			result = conn.killDocker(dockerps.get(0).getContainerId());
		} else {
			Map<Integer, String> jps = conn.jps(taskId, commandType.getJarName());
			if (MapUtils.isEmpty(jps)) {
				String str = String.format("在服务器:[%s]上停止Task[%s]出错:不存在此task", server.getIp(), taskId);
				log.error(str);
				// DuckulaUtils.returnConn(server, conn);
				return Result.getError(str);
			}
			Integer procId = jps.keySet().iterator().next();
			result = conn.kill(JvmStatus.s15, procId);
		}

		if (!result.isSuc()) {
			log.error("在服务器:[{}]上停止Task[{}]出错:{}", server.getIp(), taskId, result.getMessage());
		} else {
			if (isAuto) {
				setAuto(commandType, taskId, YesOrNo.yes);
			}
			waitUnLock(commandType, taskId, 120000);
		}
		// DuckulaUtils.returnConn(server, conn);
		return result;
	}

	private void waitUnLock(CommandType commandType, String taskId, long waitTime) {
		// 释放锁不成功，不在当前线程，只能等锁节点消失
		long begintime = System.currentTimeMillis();
		while (true) {
			List<String> taskLocks = ZkClient.getInst().getChildren(commandType.getZkPath().getPath(taskId));
			if (CollectionUtils.isNotEmpty(taskLocks)) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
				if (System.currentTimeMillis() - begintime > waitTime) {
					break;
				}
			} else {
				break;
			}
		}
	}

	private void setAuto(CommandType commandType, String taskId, YesOrNo isOpen) {
		switch (commandType) {
		case task:
			Task task = ZkUtil.buidlTask(taskId);
			task.setRun(isOpen);
			ZkClient.getInst().updateNode(ZkPath.tasks.getPath(taskId), JSONObject.toJSONString(task));
			break;
		case consumer:
			Consumer consumer = ZkUtil.buidlConsumer(taskId);
			consumer.setRun(isOpen);
			ZkClient.getInst().updateNode(ZkPath.consumers.getPath(taskId), JSONObject.toJSONString(consumer));
			break;
		default:
			break;
		}

	}

}
