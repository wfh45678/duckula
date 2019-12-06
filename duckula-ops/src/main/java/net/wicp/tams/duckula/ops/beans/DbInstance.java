package net.wicp.tams.duckula.ops.beans;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse.NodeInfo;

import lombok.Data;
import net.wicp.tams.common.constant.dic.YesOrNo;

@Data
public class DbInstance {
	private String id;
	private String url;
	private int port;
	private String user;
	private String pwd;
	private String namespaces;
	private YesOrNo isSsh = YesOrNo.no;
	private YesOrNo isRds = YesOrNo.yes;// yes表示是rds
	private List<NodeInfo> nodesFirst;
	private List<NodeInfo> nodesSecond;
	private List<NodeInfo> nodesThird;
	private YesOrNo isWhileList;// 服务器是否加入白名单
	private String remark;

	public String findMaster(List<NodeInfo> nodesList) {
		if (CollectionUtils.isEmpty(nodesList)) {
			return "";
		}
		for (NodeInfo nodeInfo : nodesList) {
			if ("Master".equals(nodeInfo.getNodeType())) {
				return nodeInfo.getNodeId();
			}
		}
		return "";
	}
}
