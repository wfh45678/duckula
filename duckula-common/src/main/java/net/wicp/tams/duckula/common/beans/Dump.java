package net.wicp.tams.duckula.common.beans;

import lombok.Data;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.YesOrNo;

@Data
public class Dump {
	private String id;
	private String taskOnlineId;
	private String cluster;// es的集群配置，就是conf下面的配置
	private YesOrNo needSend=YesOrNo.yes;//是否需要发duckula发送
	private String mappingId;
	private String db_tb;
	private String[] primarys;
	private Integer numDuan;
	private String wheresql;// where语句
	private String remark;

	private String busiPlugin;// 业务用的插件
	// 多线程配置
	private Integer baseDataNum;// 抽数据线程数
	private Integer busiNum;// 业务处理线程数
	private Integer sendNum;// 发送线程数

	public String packFromstr() {
		if (StringUtil.isNotNull(this.wheresql)) {
			this.wheresql = StringUtil.trimSpace(this.wheresql);
			if (!this.getWheresql().substring(0, 5).equalsIgnoreCase("where")) {
				this.wheresql = "where " + this.wheresql;
			}
		}
		String fromstr = String.format("from %s %s", this.getDb_tb(),
				StringUtil.isNull(this.getWheresql()) ? "where 1=1 " : this.getWheresql());
		return fromstr;
	}
}
