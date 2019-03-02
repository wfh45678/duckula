package net.wicp.tams.duckula.common.beans;

import lombok.Data;
import net.wicp.tams.common.constant.dic.YesOrNo;

/***
 * 用于consumer的jdbc插件
 * 
 * @author 偏锋书生
 *
 *         2018年6月28日
 */
@Data
public class DbInstance {
	private String id;
	private String url;
	private int port;
	private String user;
	private String pwd;
	private YesOrNo isSsh = YesOrNo.no;
	private YesOrNo isRds = YesOrNo.yes;// yes表示是rds
	private YesOrNo isWhileList;// 服务器是否加入白名单
	private String remark;

}
