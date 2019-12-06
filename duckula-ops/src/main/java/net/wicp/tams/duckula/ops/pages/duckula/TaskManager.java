package net.wicp.tams.duckula.ops.pages.duckula;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.tapestry5.annotations.OnEvent;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.json.JSONArray;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import common.kubernetes.constant.ResourcesType;
import common.kubernetes.tiller.TillerClient;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.common.apiext.json.easyuibean.EasyUINode;
import net.wicp.tams.common.apiext.json.easyuibean.EasyUINodeConf;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.callback.impl.convertvalue.ConvertValueEnum;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.EsAssit;
import net.wicp.tams.common.es.bean.IndexBean;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.component.annotation.HtmlJs;
import net.wicp.tams.component.constant.EasyUIAdd;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.FilterPattern;
import net.wicp.tams.duckula.common.constant.SenderEnum;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.pages.es.IndexManager;
import net.wicp.tams.duckula.ops.services.InitDuckula;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Slf4j
@HtmlJs(easyuiadd = { EasyUIAdd.edatagrid })
public class TaskManager {

	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Property
	private Task task;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	@Property
	@SessionState
	private String namespace;

	private boolean namespaceExists;

	public boolean isNeedServer() {
		return TaskPattern.isNeedServer();
	}

	public String getDefaultImageVersion() {
		return Conf.get("duckula.task.image.tag");
	}

	public String getDefaultNamespace() {
		String namespaceTrue = "all".equalsIgnoreCase(namespace) ? "" : namespace;
		return StringUtil.hasNull(namespaceTrue, Conf.get("common.kubernetes.apiserver.namespace.default"));
	}

	public String getColDifferent() {
		if (isNeedServer()) {
			return "{field:'hosts',width:100,title:'任务主机'}";
		} else {
			return "{field:'podStatus',width:120,title:'k8s状态'},{field:'imageVersion',width:80,title:'image版本'},{field:'namespace',width:100,title:'名称空间'}";
		}
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() {
		if (!namespaceExists) {
			return TapestryAssist.getTextStreamResponse(EasyUiAssist.getJsonForGridEmpty());
		}

		final Task taskparam = TapestryAssist.getBeanFromPage(Task.class, requestGlobals);

		List<String> taskNodes = ZkClient.getInst().getChildren(ZkPath.tasks.getRoot());
		if (CollectionUtils.isEmpty(taskNodes)) {
			return TapestryAssist.getTextStreamResponse("{}");
		}
		List<Task> tasks = CollectionFactory.newList();
		for (String taskId : taskNodes) {
			Task temp = ZkUtil.buidlTask(taskId);
			tasks.add(temp);
		}

		List<Task> retlist = (List<Task>) CollectionUtils.select(tasks, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if (object == null) {
					return false;
				}
				Task temp = (Task) object;
				if (!TaskPattern.isNeedServer() && !"all".equals(namespace)
						&& !namespace.equalsIgnoreCase(temp.getNamespace())) {
					return false;
				}
				boolean ret = true;
				if (StringUtil.isNotNull(taskparam.getId())) {
					ret = temp.getId().indexOf(taskparam.getId()) >= 0;
					if (!ret) {
						return false;
					}
				}
				if (StringUtil.isNotNull(taskparam.getIp())) {
					ret = temp.getIp().indexOf(taskparam.getIp()) >= 0;
					if (!ret) {
						return false;
					}
				}

				if (StringUtil.isNotNull(taskparam.getRules())) {
					ret = StringUtil.isNull(temp.getRules()) ? false
							: temp.getRules().indexOf(taskparam.getRules()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}

		});

		String retstr = null;
		if (isNeedServer()) {
			final Map<String, List<String>> taskRunServerMap = new HashMap<>();
			List<Server> findAllServers = duckulaAssit.findAllServers();
			for (Task task : retlist) {
				List<String> serverids = duckulaAssit.lockToServer(findAllServers, ZkPath.tasks, task.getId());
				taskRunServerMap.put(task.getId(), serverids);
			}

			IConvertValue<String> hostNumConvert = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					return String.valueOf(taskRunServerMap.get(keyObj).size());
				}
			};

			IConvertValue<String> hostNumList = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					return CollectionUtil.listJoin(taskRunServerMap.get(keyObj), ",");
				}
			};
			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "ip", "clientId", "port", "user", "pwd", "senderEnum", "receivePluginDir",
							"params", "threadNum", "dbinst", "rules", "remark", "run", "rds", "serializerEnum",
							"posListener", "busiEnum", "middlewareType", "middlewareInst", "busiPluginDir", "isSsh",
							"id,hostNum", "id,hosts", "senderEnum,senderEnum1" },
					new IConvertValue[] { null, null, null, null, null, null, null, null, null, null, null, null, null,
							null, null, null, null, null, null, null, null, null, hostNumConvert, hostNumList,
							new ConvertValueEnum(SenderEnum.class) },
					retlist.size());
		} else {
			IConvertValue<String> podStatus = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					keyObj = CommandType.task.getK8sId(keyObj);
					Map<ResourcesType, String> queryStatus = TillerClient.getInst().queryStatus(keyObj);
					String valueStr = queryStatus.get(ResourcesType.Pod);
					String colValue = ResourcesType.Pod.getColValue(valueStr, "STATUS");
					return colValue;
				}
			};
			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "ip", "clientId", "port", "user", "pwd", "senderEnum", "receivePluginDir",
							"params", "threadNum", "dbinst", "rules", "remark", "run", "rds", "serializerEnum",
							"posListener", "busiEnum", "middlewareType", "middlewareInst", "busiPluginDir", "isSsh",
							"imageVersion", "namespace", "id,podStatus", "senderEnum,senderEnum1" },
					new IConvertValue[] { null, null, null, null, null, null, null, null, null, null, null, null, null,
							null, null, null, null, null, null, null, null, null, null, null, podStatus,
							new ConvertValueEnum(SenderEnum.class) },
					retlist.size());
		}

		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSave() {
		final Task taskparam = TapestryAssist.getBeanFromPage(Task.class, requestGlobals);
		if (taskparam.getClientId() == 0) {
			taskparam.setClientId(StringUtil.buildPort(taskparam.getId()));
		}
		if (StringUtil.isNull(taskparam.getBeginTime())) {
			taskparam.setBeginTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		}
		if (StringUtil.isNotNull(taskparam.getDbinst()) && !"no".equals(taskparam.getDbinst())) {// 数据库实例
			DbInstance temp = ZkClient.getInst().getDateObj(
					String.format("%s/%s", ZkPath.dbinsts.getRoot(), taskparam.getDbinst()), DbInstance.class);
			taskparam.setIp(temp.getUrl());
			taskparam.setPort(temp.getPort());
			taskparam.setUser(temp.getUser());
			taskparam.setPwd(temp.getPwd());
			taskparam.setIsSsh(temp.getIsSsh() == null ? YesOrNo.no : temp.getIsSsh());
		}
		Stat stat = ZkUtil.exists(ZkPath.tasks, taskparam.getId());
		if (stat == null) {// 新增
			taskparam.setClientId(StringUtil.buildPort(taskparam.getId()));// 防止报“A slave with the same
																			// server_uuid/server_id”
			taskparam.setRun(YesOrNo.no);// 不立即启动，需要做其它配置
			ZkClient.getInst().createNode(ZkPath.tasks.getPath(taskparam.getId()), JSONObject.toJSONString(taskparam));
			PathChildrenCache createPathChildrenCache = ZkClient.getInst()
					.createPathChildrenCache(ZkPath.tasks.getPath(taskparam.getId()), InitDuckula.haWatcherTask);
			InitDuckula.cacheTaskListener.put(taskparam.getId(), createPathChildrenCache);
		} else {
			ZkClient.getInst().updateNode(ZkPath.tasks.getPath(taskparam.getId()), JSONObject.toJSONString(taskparam));
		}

		if (taskparam.getPosListener() == YesOrNo.no) {// 不监听pos
			InitDuckula.noPosListener.add(ZkPath.pos.getPath(taskparam.getId()));
		} else {
			InitDuckula.noPosListener.remove(ZkPath.pos.getPath(taskparam.getId()));
		}
		// init Index
		if (taskparam.getSenderEnum() == SenderEnum.es) {
			for (Rule rule : taskparam.getRuleList()) {
				if (StringUtil.isNotNull(rule.getItems().get(RuleItem.copynum))
						&& StringUtil.isNotNull(rule.getItems().get(RuleItem.partitions))) {
					String db = rule.getDbPattern().replaceAll("\\^", "").replaceAll("\\$", "")
							.replaceAll("\\[0-9\\]\\*", "");
					String tb = rule.getTbPattern().replaceAll("\\^", "").replaceAll("\\$", "")
							.replaceAll("\\[0-9\\]\\*", "");
					List<IndexBean> queryIndex = IndexManager.getESClient(taskparam.getMiddlewareInst())
							.queryIndex(rule.getItems().get(RuleItem.index));
					if (CollectionUtils.isEmpty(queryIndex) && !db.endsWith("_") && !tb.endsWith("_")) {
						java.sql.Connection conn = JdbcConnection.getConnectionMyql(taskparam.getIp(),
								taskparam.getPort(), taskparam.getUser(), taskparam.getPwd(), taskparam.getIsSsh());
						String[][] cols = MySqlAssit.getCols(conn, db, tb, YesOrNo.yes);
						try {
							conn.close();
						} catch (SQLException e1) {
						}
						String contentjson = "";
						if (ArrayUtils.isNotEmpty(cols) && !"_rowkey_".equals(cols[0][0])) {// 有主键
							contentjson = EsAssit.packIndexContent(cols[0], cols[1]);
						}
						if (StringUtil.isNull(contentjson)) {
							continue;
						}
						MappingBean proMappingBean = null;
						try {
							proMappingBean = MappingBean.proMappingBean(contentjson);
						} catch (Exception e) {
						}
						if (proMappingBean == null) {
							continue;
						}
						Result indexCreate = IndexManager.getESClient(taskparam.getMiddlewareInst()).indexCreate(
								rule.getItems().get(RuleItem.index), "_doc",
								Integer.parseInt(rule.getItems().get(RuleItem.partitions)),
								Integer.parseInt(rule.getItems().get(RuleItem.copynum)), proMappingBean);
						if (indexCreate.isSuc()) {
							Mapping mapping = new Mapping();
							mapping.setId(rule.getItems().get(RuleItem.index) + "-_doc");
							mapping.setDb(db);
							mapping.setTb(tb);
							mapping.setIndex(rule.getItems().get(RuleItem.index));
							mapping.setType("_doc");
							mapping.setContent(contentjson);
							mapping.setShardsNum(Integer.parseInt(rule.getItems().get(RuleItem.partitions)));
							mapping.setReplicas(Integer.parseInt(rule.getItems().get(RuleItem.copynum)));
							mapping.setDbinst(taskparam.getDbinst());
							Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(
									ZkPath.mappings.getPath(mapping.getId()), JSONObject.toJSONString(mapping));
							log.info("创建索引节点结果：" + createOrUpdateNode.getMessage());
						}
						log.info(rule.getItems().get(RuleItem.index) + "创建结果：" + indexCreate.getMessage());
					}
				}
			}
		}
		return req.retSuccInfo("保存Task成功");
	}

	/**
	 * 查询数据库字例
	 * 
	 */
	public TextStreamResponse onQueryInst() {
		List<String> dbs = ZkClient.getInst().getChildren(ZkPath.dbinsts.getRoot());// 所有dbs
		dbs.add(0, "no");
		String retstr = JSONUtil.getJsonForListSimple(dbs);
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onDel() {
		final Task taskparam = TapestryAssist.getBeanFromPage(Task.class, requestGlobals);
		try {
			InitDuckula.cacheTaskListener.get(taskparam.getId()).close();// 不关闭监听会导致节点删除后再创建新节点的情况
		} catch (Throwable e) {
			log.error("关闭监听失败", e);
		}
		// 删除节点监听
		Result ret = ZkUtil.del(ZkPath.tasks, taskparam.getId());
		// 删除关联的dump任务
		List<Dump> findAllDump = ZkUtil.findAllDump();
		for (Dump dump : findAllDump) {
			if (taskparam.getId().equals(dump.getTaskOnlineId())) {
				ZkUtil.del(ZkPath.dumps, dump.getId());
			}
		}
		// 删除关联的离线任务
		List<TaskOffline> allOffline = ZkUtil.findAllObjs(ZkPath.tasksofflines, TaskOffline.class);
		for (TaskOffline taskOffline : allOffline) {
			if (taskparam.getId().equals(taskOffline.getTaskOnlineId())) {
				ZkUtil.del(ZkPath.tasksofflines, taskOffline.getId());
			}
		}

		return TapestryAssist.getTextStreamResponse(ret);
	}

	public TextStreamResponse onQueryTree() throws Exception {
		String idstr = request.getParameter("id");
		String[] ipsAry = StringUtil.isNull(idstr) ? new String[0] : idstr.split(",");
		List<String> ips = CollectionFactory.newList();
		for (int i = 0; i < ipsAry.length; i++) {
			ips.add(ipsAry[i].split("\\|")[0]);
		}

		List<Server> allservers = duckulaAssit.findAllServers();
		Map<String, Integer> tempMap = duckulaAssit.serverRunTaskNum(allservers);
		for (Server server : allservers) {
			if (org.apache.commons.lang3.ArrayUtils.contains(ipsAry, server.getIp())) {
				server.setRun(true);
			} else {
				server.setRun(false);
			}
			server.setName(String.format("%s(任务数:%s)", server.getName(), tempMap.get(server.getIp())));
		}

		EasyUINodeConf conf = new EasyUINodeConf("ip", "name");
		conf.setCheckedCol("run");
		List<EasyUINode> retList = EasyUiAssist.getTreeRoot(allservers, conf);
		String retstr = EasyUiAssist.getTreeFromList(retList);
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	@SuppressWarnings("unchecked")
	@OnEvent(value = "savesel")
	private Result sava(JSONArray selIds, org.apache.tapestry5.json.JSONObject paramsObj) {
		long curtime1 = new Date().getTime();
		String idstr = paramsObj.has("id") ? paramsObj.getString("id") : null;// 旧的已启动的服务
		String taskid = paramsObj.getString("taskid");
		String[] ipsAry = StringUtil.isNotNull(idstr) ? idstr.split(",") : new String[0];
		final List<String> ips = CollectionFactory.newList();// 旧的已启动的服务
		for (int i = 0; i < ipsAry.length; i++) {
			if (StringUtil.isNotNull(ipsAry[i])) {
				ips.add(ipsAry[i].split("\\|")[0]);
			}
		}
		final List<Object> idsneed = selIds.toList();// 已选择的ip
		List<String> adds = (List<String>) CollectionUtils.select(idsneed, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return !ips.contains(object);
			}
		});

		List<String> dels = (List<String>) CollectionUtils.select(ips, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return !idsneed.contains(object);
			}
		});

		try {
			List<Server> allserver = duckulaAssit.findAllServers();
			StringBuffer errmsg = new StringBuffer();
			if (CollectionUtils.isNotEmpty(dels)) {// stop
				for (String del : dels) {
					Server curserver = selServer(allserver, del);
					Result ret = duckulaAssit.stopTask(CommandType.task, taskid, curserver, true);
					if (!ret.isSuc()) {
						errmsg.append(ret.getMessage());
					}
				}
			}
			if (CollectionUtils.isNotEmpty(adds)) {// start
				for (String add : adds) {
					Server curserver = selServer(allserver, add);
					Result ret = duckulaAssit.startTask(CommandType.task, taskid, curserver, true);
					if (!ret.isSuc()) {
						errmsg.append(ret.getMessage());
					}
				}
			}
			if (errmsg.length() > 0) {
				return Result.getError(errmsg.toString());
			}
		} catch (Exception e) {
			return Result.getError("出错:" + e.getMessage());
		}

		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 10000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return Result.getSuc();
	}

	public TextStreamResponse onStartK8sTask() {
		long curtime1 = new Date().getTime();
		String taskid = request.getParameter("taskid");
		Result ret = duckulaAssit.startTaskForK8s(CommandType.task, taskid, true);// TODO 非独立模式，
																					// pvc的初始化需解决、可以传入参数standalone
		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return TapestryAssist.getTextStreamResponse(ret);
	}

	public TextStreamResponse onGetFilter() throws KeeperException, InterruptedException {
		String taskId = request.getParameter("taskId");
		Stat stat = ZkClient.getInst().exists(ZkPath.filter.getPath(taskId));
		if (stat != null) {
			String zkDataStr = ZkClient.getInst().getZkDataStr(ZkPath.filter.getPath(taskId));
			return TapestryAssist.getTextStreamResponse(zkDataStr);
		} else {
			return TapestryAssist.getTextStreamResponse("");
		}
	}

	public TextStreamResponse onStopTask() {
		long curtime1 = new Date().getTime();
		String commandtypeStr = request.getParameter("commandtype");
		CommandType commandtype = CommandType.valueOf(CommandType.class, commandtypeStr);
		String taskid = request.getParameter("taskid");
		String serverid = request.getParameter("serverid");
		Server server = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.servers.getPath(serverid)),
				Server.class);
		Result ret = duckulaAssit.stopTask(commandtype, taskid, server, false);
		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return TapestryAssist.getTextStreamResponse(ret);
	}

	public TextStreamResponse onStopTaskForK8s() {
		long curtime1 = new Date().getTime();
		String commandtypeStr = request.getParameter("commandtype");
		CommandType commandtype = CommandType.valueOf(CommandType.class, commandtypeStr);
		String taskid = request.getParameter("taskid");
		Result ret = duckulaAssit.stopTaskForK8s(commandtype, taskid, false);
		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return TapestryAssist.getTextStreamResponse(ret);
	}

	public TextStreamResponse onRuleData() {
		String commandtypeStr = request.getParameter("ruleData");
		Task temp = new Task();
		temp.setRules(commandtypeStr);
		List<Rule> ruleList = temp.getRuleList();
		com.alibaba.fastjson.JSONArray ary = new com.alibaba.fastjson.JSONArray();
		for (Rule rule : ruleList) {
			JSONObject retobj = new JSONObject();
			retobj.put("db", rule.getDbPattern());
			retobj.put("tb", rule.getTbPattern());
			for (RuleItem ruleItem : RuleItem.values()) {
				if (rule.getItems().containsKey(ruleItem)) {
					retobj.put(ruleItem.name(), rule.getItems().get(ruleItem));
				}
			}
			ary.add(retobj);
		}
		return TapestryAssist.getTextStreamResponse(ary.toJSONString());
	}

	public TextStreamResponse onFilterRuleData() {
		String filterContext = request.getParameter("filterContext");
		Map<FilterPattern, Map<String, Map<String, Set<String>>>> packageFilterRules = FilterPattern
				.packageFilterRules(filterContext);// 设置值
		com.alibaba.fastjson.JSONArray ary = FilterPattern.getJson(packageFilterRules);
		return TapestryAssist.getTextStreamResponse(ary.toJSONString());
	}

//把过滤数据转为string
	public TextStreamResponse onFilterDataConvert() {
		String filterSaveData = request.getParameter("filterSaveData");
		String taskId = request.getParameter("taskId");
		JSONObject dgAll = JSONObject.parseObject(filterSaveData);
		com.alibaba.fastjson.JSONArray rows = dgAll.getJSONArray("rows");
		String retstr = FilterPattern.toString(rows);
		ZkClient.getInst().createOrUpdateNode(ZkPath.filter.getPath(taskId), retstr);// 保存zk
		return TapestryAssist.getTextStreamResponse(Result.getSuc(retstr));
	}

	public TextStreamResponse onDataConvert() {
		String saveDataStr = request.getParameter("saveData");
		JSONObject dgAll = JSONObject.parseObject(saveDataStr);
		com.alibaba.fastjson.JSONArray rows = dgAll.getJSONArray("rows");
		StringBuffer buff = new StringBuffer();
		for (int i = 0; i < rows.size(); i++) {
			buff.append("&");
			JSONObject tempObj = rows.getJSONObject(i);
			String db = tempObj.getString("db").replaceAll("\\^", "").replaceAll("\\$", "").replaceAll("\\[0-9\\]\\*",
					"");
			String tb = tempObj.getString("tb").replaceAll("\\^", "").replaceAll("\\$", "").replaceAll("\\[0-9\\]\\*",
					"");
			buff.append(db + "`");
			buff.append(tb + "`");
			tempObj.remove("db");
			tempObj.remove("tb");
			tempObj.remove("isNewRecord");// 这个是easyui对于新增用户自动产生的。
			JSONObject jsontrue = new JSONObject();
			for (String key : tempObj.keySet()) {
				if (StringUtil.isNotNull(tempObj.get(key))) {
					jsontrue.put(key, tempObj.get(key));
				}
			}
			buff.append(JSONObject.toJSONString(jsontrue, SerializerFeature.UseSingleQuotes));
		}
		String retstr = buff.length() > 1 ? buff.substring(1) : "";
		return TapestryAssist.getTextStreamResponse(Result.getSuc(retstr));
	}

	public void onActivate(String namespace) {
		this.namespace = namespace;
	}

	private Server selServer(List<Server> allserver, String serverid) {
		if (CollectionUtils.isEmpty(allserver)) {
			return null;
		}
		for (Server server : allserver) {
			if (server.getIp().equals(serverid)) {
				return server;
			}
		}
		return null;
	}
}
