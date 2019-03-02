package net.wicp.tams.duckula.ops.pages.duckula;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse.NodeInfo;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.others.RdsUtil;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

@Slf4j
public class DbInstanceManager {

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
		final DbInstance dbparam = TapestryAssist.getBeanFromPage(DbInstance.class, requestGlobals);
		List<DbInstance> instances = duckulaAssit.findAllDbInstances();

		List<DbInstance> retlist = (List<DbInstance>) CollectionUtils.select(instances, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				DbInstance temp = (DbInstance) object;
				boolean ret = true;
				if (StringUtil.isNotNull(dbparam.getId())) {
					ret = temp.getId().indexOf(dbparam.getId()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}

		});

		IConvertValue<Object> conver1 = newConverValue(1);
		IConvertValue<Object> conver2 = newConverValue(2);
		IConvertValue<Object> conver3 = newConverValue(3);

		String retstr = EasyUiAssist.getJsonForGrid(retlist,
				new String[] { "id", "url", "port", "user", "pwd", "isWhileList", "isSsh", "isRds", "remark",
						",hostIds1", ",hostIds2", ",hostIds3" },
				new IConvertValue[] { null, null, null, null, null, null, null, null, null, conver1, conver2, conver3 },
				retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	private IConvertValue<Object> newConverValue(final int no) {
		return new IConvertValue<Object>() {
			@Override
			public String getStr(Object keyObj) {
				DbInstance temp = (DbInstance) keyObj;
				List<NodeInfo> list = no == 1 ? temp.getNodesFirst()
						: (no == 2 ? temp.getNodesSecond() : temp.getNodesThird());
				if (CollectionUtils.isNotEmpty(list)) {
					StringBuffer buff = new StringBuffer();
					for (NodeInfo nodeInfo : list) {
						buff.append(String.format("%s:%s,", nodeInfo.getNodeType(), nodeInfo.getNodeId()));
					}
					return buff.length() > 0 ? buff.deleteCharAt(buff.length() - 1).toString() : "";
				} else {
					return "";
				}
			}
		};
	}

	public TextStreamResponse onSave() throws KeeperException, InterruptedException {
		DbInstance dbinst = TapestryAssist.getBeanFromPage(DbInstance.class, requestGlobals);
		// String dbId = request.getParameter("id");
		List<NodeInfo> nodeinfos = new ArrayList<>();
		// 可以查到到相关节点，这样就不能拉离线数据
		try {
			nodeinfos = RdsUtil.findNodeInfos(dbinst.getId());
		} catch (Throwable e) {
			String key = Conf.get("common.others.aliyun.server.accesskey");
			log.warn("不能连接到rds查找信息，当rds做主备切换及离线解析会受影响,在用的accesskey为：【{}】", key);
		}
		if (CollectionUtils.isEmpty(nodeinfos) && dbinst.getIsRds() == YesOrNo.yes) {
			return TapestryAssist
					.getTextStreamResponse(Result.getError("rds不能获取到主备ID，可能是accesskey不正确，也可能是id不是rds对应的实例ID"));
		}
		DbInstance dbInstance = JSONObject
				.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(dbinst.getId())), DbInstance.class);
		if (dbInstance == null) {// 新增
			dbinst.setNodesFirst(nodeinfos);
		} else if (!isSame(nodeinfos, dbInstance.getNodesFirst())) {// 需要替换
			dbinst.setNodesThird(dbInstance.getNodesSecond());
			dbinst.setNodesSecond(dbInstance.getNodesFirst());
			dbinst.setNodesFirst(nodeinfos);
		} else {
			dbinst.setNodesFirst(dbInstance.getNodesFirst());
			dbinst.setNodesSecond(dbInstance.getNodesSecond());
			dbinst.setNodesThird(dbInstance.getNodesThird());
		}
		Result result = ZkClient.getInst().createOrUpdateNode(ZkPath.dbinsts.getPath(dbinst.getId()),
				JSONObject.toJSONString(dbinst));
		return TapestryAssist.getTextStreamResponse(result);
	}

	/***
	 * nodeList1要不是替换nodeList2
	 * 
	 * @param nodeList1
	 * @param nodeList2
	 * @return
	 */
	private boolean isSame(List<NodeInfo> nodeList1, List<NodeInfo> nodeList2) {
		if (CollectionUtils.isEmpty(nodeList1)) {// 空的不替换
			return true;
		}
		if (CollectionUtils.isEmpty(nodeList2) && CollectionUtils.isNotEmpty(nodeList1)) {
			return false;
		}
		if (nodeList1.size() != nodeList2.size()) {
			return false;
		}
		List<String> newNodeIds = new ArrayList<>();
		for (NodeInfo nodeInfo : nodeList1) {
			newNodeIds.add(nodeInfo.getNodeId());
		}

		for (NodeInfo old : nodeList2) {
			if (!newNodeIds.contains(old.getNodeId())) {
				return false;
			}

		}
		return true;
	}

	public TextStreamResponse onDel() throws KeeperException, InterruptedException {
		final DbInstance dbInst = TapestryAssist.getBeanFromPage(DbInstance.class, requestGlobals);
		Result result = ZkClient.getInst().deleteNode(ZkPath.dbinsts.getPath(dbInst.getId()));
		return TapestryAssist.getTextStreamResponse(result);
	}
}
