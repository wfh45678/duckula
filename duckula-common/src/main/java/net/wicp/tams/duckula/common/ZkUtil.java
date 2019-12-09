package net.wicp.tams.duckula.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.duckula.common.beans.ColHis;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.Count;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.plugin.beans.Rule;

@Slf4j
public abstract class ZkUtil {

	// 只得到位点时间
	public static String[] findPosHis(String taskId) {
		List<String> children = ZkClient.getInst().getChildren(ZkPath.dbinsts.getPath(taskId));
		String[] ary = children.toArray(new String[children.size()]);
		//倒序
		Arrays.sort(ary, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return -o1.compareTo(o2);
			}
		});		
		return ary;
	}

	public static Pos getPosHis(String dbinstId, String posHisName) {
		String datapath = ZkPath.dbinsts.getPath(dbinstId) + "/" + posHisName;
		JSONObject data = ZkClient.getInst().getZkData(datapath);
		Pos retpos = JSONObject.toJavaObject(data, Pos.class);
		return retpos;
	}

	public static Stat exists(ZkPath path) {
		return ZkClient.getInst().exists(path.getRoot());
	}

	public static Stat exists(ZkPath path, String taskId) {
		return ZkClient.getInst().exists(path.getPath(taskId));
	}

	public static Result del(ZkPath path, String subNode) {
		Stat stat = exists(path, subNode);
		if (stat != null) {
			Result retStat = ZkClient.getInst().deleteNode(path.getPath(subNode));
			return retStat;
		}
		return Result.getSuc("无需删除");
	}

	public static List<String> findSubNodes(ZkPath path) {
		List<String> retlist = ZkClient.getInst().getChildren(path.getRoot());
		return retlist;
	}

	public static Task buidlTask(String taskId) {
		Task retobj = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.tasks.getPath(taskId)), Task.class);
		return retobj;
	}

	public static Mapping buidlMapping(String mappingId) {
		Mapping retobj = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.mappings.getPath(mappingId)),
				Mapping.class);
		return retobj;
	}

	public static List<Mapping> findAllIndex() {
		List<String> mappingNodes = ZkClient.getInst().getChildren(ZkPath.mappings.getRoot());
		List<Mapping> mappings = new ArrayList<>();
		if (CollectionUtils.isEmpty(mappingNodes)) {
			return mappings;
		}
		for (String mappingId : mappingNodes) {
			Mapping temp = ZkUtil.buidlMapping(mappingId);
			mappings.add(temp);
		}
		return mappings;
	}

	public static List<Dump> findAllDump() {
		List<String> dumpNodes = ZkClient.getInst().getChildren(ZkPath.dumps.getRoot());
		List<Dump> dumps = new ArrayList<>();
		if (CollectionUtils.isEmpty(dumpNodes)) {
			return dumps;
		}
		for (String dumpId : dumpNodes) {
			Dump temp = ZkUtil.buidlDump(dumpId);
			dumps.add(temp);
		}
		return dumps;
	}

	public static <T> List<T> findAllObjs(ZkPath zkPath, Class<T> classz) {
		List<String> subNodes = ZkClient.getInst().getChildren(zkPath.getRoot());
		List<T> retlist = new ArrayList<>();
		if (CollectionUtils.isEmpty(subNodes)) {
			return retlist;
		}
		for (String subNode : subNodes) {
			T retobj = ZkClient.getInst().getDateObj(zkPath.getPath(subNode), classz); // JSONObject.toJavaObject(.getZkData(zkPath.getPath(subNode)).tojson,
																						// classz);
			if (retobj != null) {
				retlist.add(retobj);
			}
		}
		return retlist;
	}

	public static TaskOffline buidlTaskOffline(String taskOfflineId) {
		TaskOffline retobj = JSONObject.toJavaObject(
				ZkClient.getInst().getZkData(ZkPath.tasksofflines.getPath(taskOfflineId)), TaskOffline.class);
		return retobj;
	}

	public static InterProcessMutex lockTaskPath(String taskId) {
		return ZkClient.getInst().lockPath(ZkPath.tasks.getPath(taskId));
	}

	public static InterProcessMutex lockConsumerPath(String consumerId) {
		return ZkClient.getInst().lockPath(ZkPath.consumers.getPath(consumerId));
	}

	public static InterProcessMutex lockDumpPath(String dumpId) {
		return ZkClient.getInst().lockPath(ZkPath.dumps.getPath(dumpId));
	}

	public static InterProcessMutex lockTaskOfflinePath(String taskId) {
		return ZkClient.getInst().lockPath(ZkPath.tasksofflines.getPath(taskId));
	}

	public static List<String> lockIps(ZkPath zkPath, String taskId) {
		List<String> nodes = ZkClient.getInst().getChildren(zkPath.getPath(taskId));
		List<String> retlist = new ArrayList<>();
		for (String node : nodes) {
			if (zkPath == ZkPath.dumps && node.equals("lastId")) {
				continue;
			}
			String zkValue = ZkClient.getInst().getZkDataStr(String.format("%s/%s", zkPath.getPath(taskId), node));
			retlist.add(zkValue);
		}
		return retlist;
	}

	public static Pos buidlPos(String taskId) {
		Pos retobj = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.pos.getPath(taskId)), Pos.class);
		return retobj;
	}

	public static Dump buidlDump(String dumpId) {
		Dump retobj = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dumps.getPath(dumpId)), Dump.class);
		return retobj;
	}

	public static Consumer buidlConsumer(String consumerId) {
		Consumer retobj = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.consumers.getPath(consumerId)),
				Consumer.class);
		return retobj;
	}

	/*
	 * public static Rule findRule(Task task,String db, String tb) { for (Rule rule
	 * : task.getRuleList()) { if (!"^*$".equals(rule.getDbPattern())) { boolean
	 * retdb = StrPattern.checkStrFormat(rule.getDbPattern(), db); if (!retdb) {
	 * continue; } } if (!"^*$".equals(rule.getTbPattern())) { boolean rettb =
	 * StrPattern.checkStrFormat(rule.getTbPattern(), tb); if (!rettb) { continue; }
	 * } return rule; } return null; }
	 */

	public static Map<String, SortedSet<ColHis>> buildCols(String dbInstName, Task task) {
		Map<String, SortedSet<ColHis>> ret = new HashMap<>();
		try {
			String dbInstRootPath = ZkPath.cols.getPath(dbInstName);
			Stat stat = ZkUtil.exists(ZkPath.cols, dbInstName);
			if (stat == null) {
				ZkClient.getInst().createNode(dbInstRootPath, null);
			}
			List<String> colsTables = ZkClient.getInst().getChildren(dbInstRootPath);
			if (CollectionUtils.isEmpty(colsTables)) {
				return ret;
			}

			CollectionUtils.filter(colsTables, new Predicate() {
				@Override
				public boolean evaluate(Object object) {
					String[] splitary = String.valueOf(object).split("\\|");
					Rule retRule = null;
					for (Rule rule : task.getRuleList()) {
						if (!"^*$".equals(rule.getDbPattern())) {
							boolean retdb = StrPattern.checkStrFormat(rule.getDbPattern().toLowerCase(), splitary[0]);
							if (!retdb) {
								continue;
							}
						}
						if (!"^*$".equals(rule.getTbPattern())) {
							boolean rettb = StrPattern.checkStrFormat(rule.getTbPattern().toLowerCase(), splitary[1]);
							if (!rettb) {
								continue;
							}
						}
						retRule = rule;
						break;
					}
					return retRule != null;
				}
			});

			for (String colsTable : colsTables) {
				String path = String.format("%s/%s", dbInstRootPath, colsTable);
				List<ColHis> list = JSONObject.parseArray(ZkClient.getInst().getZkDataStr(path), ColHis.class);
				SortedSet<ColHis> templist = new TreeSet<>();
				templist.addAll(list);
				ret.put(colsTable, templist);
			}
		} catch (Exception e) {
			log.error("组装cols出错", e);
		}
		return ret;
	}

	public static void updateCols(String dbInstName, String colsTable, SortedSet<ColHis> list) {
		String keyPath = String.format("%s/%s/%s", ZkPath.cols.getRoot(), dbInstName, colsTable);
		ZkClient.getInst().createOrUpdateNode(keyPath, JSONArray.toJSONString(list));
	}

	public static void updatePos(String taskId, Pos pos) {
		ZkClient.getInst().createOrUpdateNode(ZkPath.pos.getPath(taskId), JSONObject.toJSONString(pos));
	}

	public static void updateCount(String taskId, Count count) {
		ZkClient.getInst().createOrUpdateNode(ZkPath.counts.getPath(taskId), JSONObject.toJSONString(count));
	}

	public static void updateCos(String taskId, ColHis colhis) {
		try {
			String jsonstr = ZkClient.getInst().getZkDataStr(ZkPath.cols.getColsPath(taskId, colhis));
			List<ColHis> list = JSONObject.parseArray(jsonstr, ColHis.class);
			boolean isInclude = false;
			for (ColHis colHis2 : list) {
				if (colHis2.getTime() == colhis.getTime()) {
					isInclude = true;
					break;
				}
			}
			if (!isInclude) {
				list.add(colhis);
				Collections.sort(list);
				ZkClient.getInst().createOrUpdateNode(ZkPath.cols.getColsPath(taskId, colhis),
						JSONObject.toJSONString(list));
			}
		} catch (Exception e) {
			log.error("更新列失败", e);
			throw new IllegalStateException();
		}

	}
}
