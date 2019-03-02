package net.wicp.tams.duckula.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;

@Slf4j
public class ZkClient {
	private static ZkClient inst = new ZkClient();
	private final CuratorFramework curator;

	private ZkClient() {
		try {
			// 分布式锁用的默认值,不设置就会取本机IP地址
			curator = CuratorFrameworkFactory.builder().defaultData(OSinfo.findIpAddressTrue().getBytes("UTF-8"))
					.connectString(Conf.get("common.others.zookeeper.constr"))
					.sessionTimeoutMs(Integer.parseInt(Conf.get("common.others.zookeeper.sleepTimeMs")))
					.connectionTimeoutMs(Integer.parseInt(Conf.get("common.others.zookeeper.sessionTimeoutMs")))
					.retryPolicy(new ExponentialBackoffRetry(
							Integer.parseInt(Conf.get("common.others.zookeeper.sleepTimeMs")),
							Integer.parseInt(Conf.get("common.others.zookeeper.maxRetries"))))
					.build();
		} catch (Exception e) {
			throw new RuntimeException("初始化client错误");
		}
		/*
		 * curator =
		 * CuratorFrameworkFactory.newClient(Conf.get("common.others.zookeeper.constr"),
		 * Integer.parseInt(Conf.get("common.others.zookeeper.sleepTimeMs")),
		 * Integer.parseInt(Conf.get("common.others.zookeeper.sessionTimeoutMs")), new
		 * ExponentialBackoffRetry(Integer.parseInt(Conf.get(
		 * "common.others.zookeeper.sleepTimeMs")),
		 * Integer.parseInt(Conf.get("common.others.zookeeper.maxRetries"))));
		 */
		curator.start();
	}

	public static ZkClient getInst() {
		return ZkClient.inst;
	}

	public Stat exists(String path) {
		try {
			return curator.checkExists().forPath(path);
		} catch (Exception e) {
			log.error("查找task配置错误", e);
			return null;
		}
	}

	public Result watchPath(String path, Watcher watcher) {
		try {
			curator.getData().usingWatcher(watcher).inBackground().forPath(path);
			return Result.getSuc();
		} catch (Exception e) {
			log.error("设置观察错误", e);
			return Result.getError(e.getMessage());
		}
	}

	public InterProcessMutex lockPath(String path) {
		InterProcessMutex retLock = new InterProcessMutex(curator, path);
		return retLock;
	}

	public List<String> lockValueList(InterProcessMutex interProcessMutex) {
		try {
			List<String> retlist = new ArrayList<>();
			Collection<String> values = interProcessMutex.getParticipantNodes();
			for (String value : values) {
				String val = getZkDataStr(value);
				retlist.add(val);
			}
			return retlist;
		} catch (Exception e) {
			log.error("获取锁信息失败", e);
			return null;
		}
	}

	/**
	 * Path Cache：监视一个路径下孩子结点的创建、新增,删除以及结点数据的更新。
	 * 产生的事件会传递给注册的PathChildrenCacheListener。 Node
	 * Cache：监视一个结点的创建、更新、删除，并将结点的数据缓存在本地。 Tree Cache：Path Cache和Node
	 * Cache的“合体”，监视路径下的创建、更新、删除事件，并缓存路径下所有孩子结点的数据。
	 * 
	 */
	public PathChildrenCache createPathChildrenCache(String path, PathChildrenCacheListener children) {
		try {
			PathChildrenCache childrenCache = new PathChildrenCache(curator, path, true);
			childrenCache.getListenable().addListener(children);
			childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
			return childrenCache;
		} catch (Exception e) {
			log.error("设置观察错误", e);
			return null;
		}

	}

	public List<String> getChildren(String path) {
		try {
			List<String> colsTables = curator.getChildren().forPath(path);
			return colsTables;
		} catch (Exception e) {
			log.error("查看子节点错误", e);
			return null;
		}
	}

	public CuratorFramework getCurator() {
		return curator;
	}

	public String createNode(String path, String value, boolean isTemp) {
		path = StringUtil.trimSpace(path);
		if (StringUtil.isNull(path)) {
			return null;
		}
		value = StringUtil.trimSpace(value);
		value = StringUtil.isNull(value) ? null : value;
		try {
			String pathret = null;
			CreateBuilder create = curator.create();
			if (isTemp) {
				create.withProtection().withMode(CreateMode.EPHEMERAL);
			}
			if (value == null) {
				pathret = create.forPath(path,"".getBytes());//不传会把本机IP做为context
			} else {
				pathret = create.forPath(path, value.getBytes("UTF-8"));
			}
			return pathret;
		} catch (Exception e) {
			log.error("创建结点错误", e);
			return null;
		}
	}

	public String createNode(String path, String value) {
		return createNode(path, value, false);
	}

	public Stat updateNode(String path, String data) {
		try {
			return curator.setData().forPath(path, data.getBytes("UTF-8"));
		} catch (Exception e) {
			log.error("更新数据错误", e);
			return null;
		}
	}

	public Result createOrUpdateNode(String path, String data) {
		try {
			Stat stat = curator.checkExists().forPath(path);
			if (stat == null) {
				String retpath = createNode(path, data);
				if (retpath == null) {
					return Result.getError("创建节点错误");
				}
			} else {
				Stat statret = updateNode(path, data);
				if (statret == null) {
					return Result.getError("更新节点错误");
				}
			}
			return Result.getSuc();
		} catch (Exception e) {
			log.error("更新数据错误", e);
			return Result.getError(e.getMessage());
		}
	}

	public <T> Result createOrUpdateNodeForJson(String path, T obj) {
		String jsonstr = JSONObject.toJSONString(obj);
		return createOrUpdateNode(path, jsonstr);
	}

	/***
	 * 创建多路径节点
	 * 
	 * @param path
	 */
	public void createMultilevelNode(String path) {
		if (StringUtil.isNull(path) || !path.startsWith("/")) {
			throw new IllegalArgumentException("path错误");
		}
		path = path.replace("\\", "/");
		String[] paths = path.split("/");

		try {
			for (String ele : paths) {
				if (StringUtil.isNull(ele)) {
					continue;
				}
				String tempstr = "/" + ele;
				int idx = path.indexOf(tempstr);
				String tempPath = path.substring(0, idx + tempstr.length());
				Stat stat = curator.checkExists().forPath(tempPath);
				if (stat == null) {
					curator.create().forPath(tempPath);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String getZkDataStr(String path) {
		try {
			byte[] databin = curator.getData().forPath(path);
			return new String(databin, "UTF-8");
		} catch (Exception e) {
			log.error("查找task配置错误", e);
			return null;
		}
	}

	/***
	 * 从zk上得到java对象
	 * 
	 * @param path
	 * @param clazz
	 * @return
	 */
	public <T> T getDateObj(String path, Class<T> clazz) {
		String data = getZkDataStr(path);
		return JSONObject.parseObject(StringUtil.isNull(data) ? "{}" : data, clazz);
	}

	public Result deleteNode(String path) {
		try {
			Stat stat = new Stat();
			curator.getData().storingStatIn(stat).forPath(path);
			curator.delete().deletingChildrenIfNeeded().withVersion(stat.getVersion()).forPath(path);
			return Result.getSuc();
		} catch (Exception e) {
			log.error("删除数据错误", e);
			return Result.getError("删除数据错误:" + e.getMessage());
		}
	}

	/***
	 * 得到json数据
	 * 
	 * @param path
	 * @return
	 */
	public JSONObject getZkData(String path) {
		return JSONObject.parseObject(getZkDataStr(path));
	}
}
