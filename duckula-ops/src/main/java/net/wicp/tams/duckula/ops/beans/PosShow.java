package net.wicp.tams.duckula.ops.beans;

import java.util.Date;

import lombok.Data;
import net.wicp.tams.common.constant.DateFormatCase;

@Data
public class PosShow {
	private String id;
	private String gtids;
	private long masterServerId;
	private String fileName;
	private long pos;
	private long time;
	private boolean ishalf;// 是否一个完整的gtid

	//
	private String lockIPs;
	private int hostNum;// true:在运行 false: 不在运行

	public String getTimeStr() {
		if (time <= 0) {
			return "";
		}
		Date sql = new Date(time * 1000L);
		String retstr = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(sql);
		return retstr;
	}
}
