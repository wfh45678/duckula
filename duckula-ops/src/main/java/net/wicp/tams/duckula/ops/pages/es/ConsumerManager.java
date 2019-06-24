package net.wicp.tams.duckula.ops.pages.es;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.tapestry5.annotations.OnEvent;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.json.JSONArray;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSONObject;

import common.kubernetes.constant.ResourcesType;
import common.kubernetes.tiller.TillerClient;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.SenderConsumerEnum;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.services.InitDuckula;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Slf4j
public class ConsumerManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	public boolean isNeedServer() {
		return TaskPattern.isNeedServer();
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
		final Consumer consumerparam = TapestryAssist.getBeanFromPage(Consumer.class, requestGlobals);
		List<Consumer> consumers = ZkUtil.findAllObjs(ZkPath.consumers, Consumer.class);
		List<Consumer> retlist = (List<Consumer>) CollectionUtils.select(consumers, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Consumer temp = (Consumer) object;
				boolean ret = true;
				if (StringUtil.isNotNull(consumerparam.getTopic())) {
					ret = temp.getTopic().indexOf(consumerparam.getTopic()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}
		});
		
		CollectionUtils.filter(retlist, new Predicate() {			
			@Override
			public boolean evaluate(Object object) {
				return object!=null&&StringUtil.isNotNull(((Consumer)object).getId());
			}
		});

		String retstr = null;
		if (isNeedServer()) {
			final Map<String, List<String>> taskRunServerMap = new HashMap<>();
			List<Server> findAllServers = duckulaAssit.findAllServers();
			for (Consumer consumer : retlist) {
				List<String> serverids = duckulaAssit.lockToServer(findAllServers, ZkPath.consumers, consumer.getId());
				taskRunServerMap.put(consumer.getId(), serverids);
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

			Map<String, IConvertValue<String>> convertsMap = new HashMap<>();
			convertsMap.put("hostNum", hostNumConvert);
			convertsMap.put("hosts", hostNumList);
			retstr = EasyUiAssist.getJsonForGridAlias(retlist, new String[] { "id,hostNum", "id,hosts" }, convertsMap,
					retlist.size());// .getJsonForGridAlias(retlist, retlist.size());
		}else {
			IConvertValue<String> podStatus = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					keyObj=CommandType.consumer.getK8sId(keyObj);
					Map<ResourcesType, String> queryStatus = TillerClient.getInst().queryStatus(keyObj);
					String valueStr = queryStatus.get(ResourcesType.Pod);
					String colValue = ResourcesType.Pod.getColValue(valueStr, "STATUS");
					return colValue;
				}
			};
			
			IConvertValue<String> imageVersionConv = new IConvertValue<String>() {
				@Override
				public String getStr(String taskOnlineId) {
					Task buidlTask = ZkUtil.buidlTask(taskOnlineId);
					if(buidlTask==null) {
						return "找不到关联的task";
					}else {
						return buidlTask.getImageVersion();
					}
				}
			};
			
			IConvertValue<String> namespaceConv = new IConvertValue<String>() {
				@Override
				public String getStr(String taskOnlineId) {
					Task buidlTask = ZkUtil.buidlTask(taskOnlineId);					
					if(buidlTask==null) {
						return "找不到关联的task";
					}else {
						return buidlTask.getNamespace();
					}					
				}
			};
			Map<String, IConvertValue<String>> convertsMap = new HashMap<>();
			convertsMap.put("podStatus", podStatus);
			convertsMap.put("imageVersion", imageVersionConv);
			convertsMap.put("namespace", namespaceConv);
			retstr = EasyUiAssist.getJsonForGridAlias(retlist, new String[] { "id,podStatus","taskOnlineId,imageVersion","taskOnlineId,namespace" }, convertsMap,
					retlist.size());// .getJsonForGridAlias(retlist, retlist.size());
		}
		return TapestryAssist.getTextStreamResponse(retstr);
	}
	

	
	
	
	public TextStreamResponse onStartK8sTask() {
		long curtime1 = new Date().getTime();
		String taskid = request.getParameter("taskid");
		Result ret = duckulaAssit.startTaskForK8s(CommandType.consumer, taskid);// TODO pvc的初始化需解决、可以传入参数standalone
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
	

	public TextStreamResponse onSave() {
		final Consumer consumerparam = TapestryAssist.getBeanFromPage(Consumer.class, requestGlobals);
		List<Rule> ruleList = consumerparam.getRuleList();
		for (Rule rule : ruleList) {
			if (StringUtil.isNull(rule.getItems().get(RuleItem.key))) {
				return TapestryAssist.getTextStreamResponse(Result
						.getError(String.format("db:%s,tb:%s,需要设置idkey", rule.getDbPattern(), rule.getTbPattern())));
			}

			if (consumerparam.getSenderConsumerEnum() == SenderConsumerEnum.es
					&& StringUtil.isNull(rule.getItems().get(RuleItem.index))) {
				return TapestryAssist.getTextStreamResponse(Result.getError(
						String.format("db:%s,tb:%s,es发送者需要设置index", rule.getDbPattern(), rule.getTbPattern())));
			}

			if (consumerparam.getSenderConsumerEnum() == SenderConsumerEnum.jdbc
					&& (StringUtil.isNull(rule.getItems().get(RuleItem.dbinstanceid))
							|| StringUtil.isNull(rule.getItems().get(RuleItem.dbtb)))) {
				return TapestryAssist.getTextStreamResponse(Result.getError(String
						.format("db:%s,tb:%s,jdbc发送者需要设置dbinstanceid和dbtb", rule.getDbPattern(), rule.getTbPattern())));
			}
		}
		Stat stat = ZkUtil.exists(ZkPath.consumers, consumerparam.getId());
		if (stat == null) {// 新增	
			consumerparam.setRun(YesOrNo.no);//不立即启动，需要做其它配置
			ZkClient.getInst().createNode(ZkPath.consumers.getPath(consumerparam.getId()), JSONObject.toJSONString(consumerparam));
			PathChildrenCache createPathChildrenCache = ZkClient.getInst()
					.createPathChildrenCache(ZkPath.consumers.getPath(consumerparam.getId()), InitDuckula.haWatcherConsumer);
			InitDuckula.cacheConsumerListener.put(consumerparam.getId(), createPathChildrenCache);
		} else {
			ZkClient.getInst().updateNode(ZkPath.consumers.getPath(consumerparam.getId()), JSONObject.toJSONString(consumerparam));
		}		
		//Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(
		//		ZkPath.consumers.getPath(consumerparam.getId()), JSONObject.toJSONString(consumerparam));
		return req.retSuccInfo("保存consumer成功");
	}

	public TextStreamResponse onDel() {
		String id = request.getParameter("id");
		try {
			InitDuckula.cacheConsumerListener.get(id).close();// 不关闭监听会导致节点删除后再创建新节点的情况
		} catch (IOException e) {
			log.error("关闭监听失败", e);
		}
		Result del = ZkUtil.del(ZkPath.consumers, id);
		return TapestryAssist.getTextStreamResponse(del);
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
					Result ret = duckulaAssit.stopTask(CommandType.consumer, taskid, curserver, true);
					if (!ret.isSuc()) {
						errmsg.append(ret.getMessage());
					}
				}
			}
			if (CollectionUtils.isNotEmpty(adds)) {// start
				for (String add : adds) {
					Server curserver = selServer(allserver, add);
					Result ret = duckulaAssit.startTask(CommandType.consumer, taskid, curserver, true);
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
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return Result.getSuc();
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
