package net.wicp.tams.duckula.ops.pages.duckula;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSONObject;

import common.kubernetes.constant.ResourcesType;
import common.kubernetes.tiller.TillerClient;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.PosShow;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

public class OpsManager {

	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IDuckulaAssit duckulaAssit;

	@Inject
	private IReq req;

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() throws KeeperException, InterruptedException {
		final Task taskparam = TapestryAssist.getBeanFromPage(Task.class, requestGlobals);
		List<PosShow> taskPosList = duckulaAssit.findAllPosForTasks();
		List<PosShow> retlist = (List<PosShow>) CollectionUtils.select(taskPosList, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				PosShow temp = (PosShow) object;
				boolean ret = true;
				if (StringUtil.isNotNull(taskparam.getId())) {
					ret = temp.getId().indexOf(taskparam.getId()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}

		});
		List<Server> findAllServers = duckulaAssit.findAllServers();
		for (PosShow posShow : retlist) {
			List<String> ips = ZkUtil.lockIps(ZkPath.tasks, posShow.getId());
			if (CollectionUtils.isEmpty(ips)) {
				posShow.setHostNum(0);
			} else {
				Task buidlTask = ZkUtil.buidlTask(posShow.getId());
				List<String> lockToServer = duckulaAssit.lockToServer(findAllServers, ZkPath.tasks, buidlTask.getId());
				posShow.setLockIPs(CollectionUtil.listJoin(lockToServer, ","));
				posShow.setHostNum(ips.size());
			}
		}

		String retstr = EasyUiAssist.getJsonForGrid(retlist,
				new String[] { "id", "gtids", "masterServerId", "fileName", "pos", "time", "timeStr", "lockIPs",
						"hostNum" },
				new IConvertValue[] { null, null, null, null, null, null, null, null, null }, retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onHealth() throws KeeperException, InterruptedException {
		List<PosShow> taskPosList = duckulaAssit.findAllPosForTasks();
		JSONObject retjson = new JSONObject();
		JSONObject tasksjson = new JSONObject();
		boolean issucess = true;
		List<Server> findAllServers = duckulaAssit.findAllServers();
		for (PosShow posShow : taskPosList) {
			Task buidlTask = ZkUtil.buidlTask(posShow.getId());
			if (TaskPattern.isNeedServer()) {
				List<String> ips = ZkUtil.lockIps(ZkPath.tasks, posShow.getId());
				if (CollectionUtils.isEmpty(ips)) {
					posShow.setHostNum(0);
				} else {
					List<String> lockToServer = duckulaAssit.lockToServer(findAllServers, ZkPath.tasks,
							buidlTask.getId());
					posShow.setLockIPs(CollectionUtil.listJoin(lockToServer, ","));
					posShow.setHostNum(ips.size());
				}
			} else {
				//////////////////////////////////////
				String keyObj = CommandType.task.getK8sId(posShow.getId());
				Map<ResourcesType, String> queryStatus = TillerClient.getInst().queryStatus(keyObj);
				String valueStr = queryStatus.get(ResourcesType.Pod);
				String colValue = ResourcesType.Pod.getColValue(valueStr, "STATUS");
				if (StringUtil.isNull(colValue)) {
					posShow.setHostNum(0);
				} else if ("Running".equals(colValue)) {// 正在运行
					posShow.setHostNum(1);
					posShow.setPodStatus(colValue);
				} else {
					posShow.setHostNum(0);
					posShow.setPodStatus(colValue);
				}
			}
			int hostNum = posShow.getHostNum();
			JSONObject taskobj = new JSONObject();

			if (hostNum == 0) {
				if (buidlTask.getRun() == YesOrNo.yes) {
					taskobj.put("status", "DOWN");
					taskobj.put("delay", "已停机");
					taskobj.put("pos", "0");
					issucess = false;
					if (!TaskPattern.isNeedServer()) {
						taskobj.put("podStatus", posShow.getPodStatus());
					}
					tasksjson.put(posShow.getId(), taskobj);
				}
			} else {
				taskobj.put("pos",
						DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date(posShow.getTime() * 1000)));
				long defTimes = new Date().getTime() - posShow.getTime() * 1000;
				if (defTimes >= 1000 * 60 * 10) {
					taskobj.put("status", "DOWN");
					long min = defTimes / (1000 * 60);
					taskobj.put("delay", min + "Minutes");
					issucess = false;
				} else {
					taskobj.put("status", "UP");
				}
				tasksjson.put(posShow.getId(), taskobj);
			}
		}
		tasksjson.put("status", issucess ? "UP" : "DOWN");
		retjson.put("tasks", tasksjson);

		boolean consumersucess = true;
		JSONObject consumersjson = new JSONObject();
		List<Consumer> consumers = ZkUtil.findAllObjs(ZkPath.consumers, Consumer.class);
		for (Consumer consumer : consumers) {
			JSONObject consumerobj = new JSONObject();
			consumerobj.put("run", consumer.getRun().name());
			consumerobj.put("groupId", consumer.getGroupId());
			List<String> serverids = duckulaAssit.lockToServer(findAllServers, ZkPath.consumers, consumer.getId());
			consumerobj.put("runserver", CollectionUtil.listJoin(serverids, ","));
			if (CollectionUtils.isEmpty(serverids)) {
				consumersucess = false;
			}
			consumerobj.put("status", CollectionUtils.isNotEmpty(serverids) ? "UP" : "DOWN");
			consumersjson.put(consumer.getId(), consumerobj);
		}
		consumersjson.put("status", consumersucess ? "UP" : "DOWN");
		retjson.put("consumers", consumersjson);
		return TapestryAssist.getTextStreamResponse(retjson.toJSONString());
	}

	public TextStreamResponse onQueryPos() throws KeeperException, InterruptedException {
		PosShow posshow = TapestryAssist.getBeanFromPage(PosShow.class, requestGlobals);
		if (StringUtil.isNull(posshow.getId())) {
			return TapestryAssist.getTextStreamResponseEmpty();
		}
		// String path = String.format("%s/%s", duckulaAssit.getPathPos(),
		// posshow.getId());
		// String posData = zk.getData(path);
		// Pos pos = JSONObject.parseObject(posData, Pos.class);

		List<Pos> selPos = DuckulaUtils.readPosLogReverse(posshow.getMasterServerId(),
				Conf.getInt("duckula.ops.pos.listener.querynum"));
		String retstr = EasyUiAssist.getJsonForGrid(selPos,
				new String[] { "gtids", "masterServerId", "fileName", "pos", "time", "timeStr" }, selPos.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSavePos() throws KeeperException, InterruptedException {
		Pos pos = TapestryAssist.getBeanFromPage(Pos.class, requestGlobals);
		String taskId = request.getParameter("id");
		ZkUtil.updatePos(taskId, pos);
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public TextStreamResponse onDelPos() {
		PosShow posshow = TapestryAssist.getBeanFromPage(PosShow.class, requestGlobals);
		Result result = ZkUtil.del(ZkPath.pos, posshow.getId());
		return TapestryAssist.getTextStreamResponse(result);
	}

}
