package net.wicp.tams.duckula.ops.services;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSONObject;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.TarUtil;
import net.wicp.tams.common.constant.EPlatform;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;

@Slf4j
public class InitDuckula implements ServletContextListener {
	public static Map<String, PathChildrenCache> cacheTaskListener = new HashMap<>();// 在删除时需要关闭，不关闭监听会导致节点删除后再创建新节点的情况

	public static Map<String, PathChildrenCache> cacheConsumerListener = new HashMap<>();

	public static HaWatcher haWatcherTask = new HaWatcher(CommandType.task);// 新增任务需要

	public static HaWatcher haWatcherConsumer = new HaWatcher(CommandType.consumer);// 新增consumer需要

	public static Set<String> noPosListener = new HashSet<>();// 不需要监听位点的任务

	private void initOps(String duckulaData, ServletContextEvent servletContextEvent) throws Exception {
		// 1、确保文件存在
		File file = new File(duckulaData);
		File parentFile = file.getParentFile();
		if (!parentFile.exists()) {
			FileUtils.forceMkdir(parentFile);
		}
		// 2、解压tar
		String[] list = file.list();
		if (list.length < 2) {
			String duckulaDataPath = servletContextEvent.getServletContext().getRealPath("/resource/duckula-data.tar");
			String tmp = IOUtil.mergeFolderAndFilePath(parentFile.getPath(), "/tmp");
			File tmpFile = new File(tmp);
			if (tmpFile.exists()) {
				FileUtils.forceDelete(tmpFile);
			}
			TarUtil.decompress(duckulaDataPath, tmp);// parentFile.getParent()
			FileUtils.copyDirectory(new File(IOUtil.mergeFolderAndFilePath(tmp, "duckula-data")), file,
					new FileFilter() {
						@Override
						public boolean accept(File pathname) {
							return !"logs".equals(pathname.getName());
						}
					});
			FileUtils.forceDelete(tmpFile);
		}
	}

	@Override
	public void contextInitialized(ServletContextEvent paramServletContextEvent) {
		String duckulaData = System.getenv("DUCKULA_DATA");
		if (StringUtil.isNull(duckulaData)) {
			// 设置环境变量
			String cmdtrue = "";
			duckulaData = "/data/duckula-data";
			if (OSinfo.getOSname() == EPlatform.Windows) {
				cmdtrue = "setx DUCKULA_DATA " + duckulaData;
			} else if (OSinfo.getOSname() == EPlatform.Linux) {
				cmdtrue = "echo -e  \"DUCKULA_DATA=" + duckulaData
						+ "\\nexport PATH  DUCKULA_DATA\" >  /etc/profile.d/duckula-ops.sh";
			} else {
				log.error("没有设置环境变量，请手动设置：DUCKULA_DATA");
				LoggerUtil.exit(JvmStatus.s9);
			}
			try {
				final Process ps = Runtime.getRuntime().exec(cmdtrue);
				ps.waitFor(3, TimeUnit.SECONDS);
				IOUtil.slurp(ps.getInputStream(), Conf.getSystemEncode());
			} catch (IOException ioe) {
				log.error("IO异常，文件有误", ioe);
				log.error("没有设置环境变量，请手动设置：DUCKULA_DATA");
				LoggerUtil.exit(JvmStatus.s9);
			} catch (InterruptedException e) {
				log.error("中断异常", e);
				log.error("没有设置环境变量，请手动设置：DUCKULA_DATA");
				LoggerUtil.exit(JvmStatus.s9);
			}
			log.error("已设置环境变量：DUCKULA_DATA=" + duckulaData + "，请重新启动。");
			LoggerUtil.exit(JvmStatus.s9);
		}
		log.info("use  env:{}", duckulaData);

		if (TaskPattern.isNeedServer()) {// 只有在需要服务时才初始化DuckulaData
			try {
				initOps(duckulaData, paramServletContextEvent);
			} catch (Exception e) {
				log.error("初始化data数据失败", e);
				LoggerUtil.exit(JvmStatus.s9);
			}
		}
		CommandType.setZkProps();
		Properties opsProp = IOUtil
				.fileToProperties(new File(String.format("%s/conf/duckula-ops.properties", duckulaData)));
		if (opsProp.isEmpty()) {
			log.error("没有取得属性文件,请确认设置了DUCKULA_DATA环境变量");
			LoggerUtil.exit(JvmStatus.s9);
			throw new RuntimeException("没有取得属性文件,请确认设置了DUCKULA_DATA环境变量");
		}
		Conf.overProp(opsProp);
		CommandType.setUserProps();
		// 处理任务启动模式tiller/k8s/process&docker
		String taskpatternstr = System.getenv("taskpattern");
		if (StringUtil.isNull(taskpatternstr)) {
			taskpatternstr = Conf.get("duckula.ops.starttask.pattern");
		}
		TaskPattern taskPattern = TaskPattern.get(taskpatternstr);
		log.info("task启动模式：", taskPattern);
		Properties newProps = new Properties();
		newProps.put("duckula.ops.starttask.pattern", taskPattern.name());
		if (TaskPattern.tiller == TaskPattern.getCurTaskPattern()
				&& StringUtil.isNull(Conf.get("common.kubernetes.tiller.serverip"))) {// 是tiller方式部署且没有配置serverip
			newProps.put("common.kubernetes.tiller.serverip", "tiller-deploy.kube-system");
			newProps.put("common.kubernetes.tiller.port",
					StringUtil.hasNull(Conf.get("common.kubernetes.tiller.port"), "44134"));
		}

		Conf.overProp(newProps);
		// 3、 使用的磁盘 claimName，启动task时需要它传入
		String claimName = System.getenv("claimname");
		if (StringUtil.isNull(claimName)) {
			claimName = Conf.get("duckula.ops.starttask.claimname");
		}
		if (StringUtil.isNotNull(claimName)) {// ops必须要，但task可以不用设置
			newProps.put("duckula.ops.starttask.claimname", claimName);
		}

		// 初始化目录
		Stat stat = ZkClient.getInst().exists(Conf.get("duckula.zk.rootpath"));
		if (stat == null) {
			ZkClient.getInst().createNode(Conf.get("duckula.zk.rootpath"), "the duckula root");
		}
		for (ZkPath zkPath : ZkPath.values()) {
			Stat statTemp = ZkUtil.exists(zkPath);
			if (statTemp == null) {
				ZkClient.getInst().createNode(zkPath.getRoot(), null);
			}
		}

		if (TaskPattern.isNeedServer()) {
			// task监听
			List<String> allTaskIds = ZkUtil.findSubNodes(ZkPath.tasks);
			for (String taskId : allTaskIds) {
				Task task = ZkUtil.buidlTask(taskId);
				if (task != null) {// 如里有监听将不能正常删除节点，删除后后重建一个空节点
					PathChildrenCache createPathChildrenCache = ZkClient.getInst()
							.createPathChildrenCache(ZkPath.tasks.getPath(taskId), haWatcherTask);
					cacheTaskListener.put(taskId, createPathChildrenCache);
					if (task.getPosListener() != null && task.getPosListener() == YesOrNo.no) {
						noPosListener.add(ZkPath.pos.getPath(task.getId()));
					}
				}
			}
			// consumer监听
			List<String> allConsumersIds = ZkUtil.findSubNodes(ZkPath.consumers);
			for (String consumerId : allConsumersIds) {
				Consumer buidlConsumer = ZkUtil.buidlConsumer(consumerId);
				if (buidlConsumer != null) {// 如里有监听将不能正常删除节点，删除后后重建一个空节点
					PathChildrenCache createPathChildrenCache = ZkClient.getInst()
							.createPathChildrenCache(ZkPath.consumers.getPath(consumerId), haWatcherConsumer);
					cacheConsumerListener.put(consumerId, createPathChildrenCache);
				}
			}
		}

		// 日志监听
		if (Conf.getBoolean("duckula.ops.pos.listener.enable")) {
			// 监听pos
			PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					ChildData data = event.getData();

					switch (event.getType()) {
					case CHILD_ADDED:
						System.out.println("CHILD_ADD : " + data.getPath() + "  数据:" + data.getData());
						break;
					case CHILD_REMOVED:
						System.out.println("CHILD_REMOVED : " + data.getPath() + "  数据:" + data.getData());
						break;
					case CHILD_UPDATED:
						if (!noPosListener.contains(data.getPath())) {// 不需要监听binlogpos
							alterDate(data.getPath(), data.getData());
						}
						break;
					default:
						break;
					}
				}

				private final Map<String, Long> beginTimeMap = new HashMap<>();

				private final int interval = Conf.getInt("duckula.ops.pos.listener.interval") * 1000;// 默认5分钟打一次分钟打一次

				private void alterDate(String path, byte[] data) throws UnsupportedEncodingException {
					long curTime = System.currentTimeMillis();
					long beginTime = beginTimeMap.containsKey(path) ? beginTimeMap.get(path) : 0;
					if (curTime - beginTime > interval) {
						String posvalue = new String(data, "UTF-8");
						Pos pos = JSONObject.parseObject(posvalue, Pos.class);
						long masterServerId = pos.getMasterServerId();
						Logger logger = DuckulaUtils.getPosLog(masterServerId);
						logger.info(posvalue);
						beginTimeMap.put(path, curTime);
					}
				}
			};
			ZkClient.getInst().createPathChildrenCache(ZkPath.pos.getRoot(), childrenCacheListener);
		}

		ConfUtil.printlnASCII();
	}

	@Override
	public void contextDestroyed(ServletContextEvent paramServletContextEvent) {
		System.out.println("bb");
	}

}
