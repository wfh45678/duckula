package net.wicp.tams.duckula.common.beans;

import java.util.Date;

import lombok.Data;
import net.wicp.tams.common.constant.DateFormatCase;

/***
 * 位点信息
 * 
 * @author zhoujunhui
 *
 */
@Data
public class Pos implements Comparable<Pos>, Cloneable {
	private String gtids;
	private long masterServerId;
	private String fileName;
	private long pos;
	private long time;
	private boolean ishalf;//是否一个完整的gtid

	public String getTimeStr() {
		if (time <= 0) {
			return "";
		}
		Date sql = new Date(time * 1000L);
		String retstr = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(sql);
		return retstr;
	}

	@Override
	public int compareTo(Pos o) {
		if (time == 0) {
			return 1;
		}
		if (o.getTime() == 0) {
			return -1;
		}
		return (int) (o.getTime() - time);// 时间倒序
	}

	@Override
	public boolean equals(Object obj) {
		Pos temp = (Pos) obj;
		return time == temp.getTime();
	}

	@Override
	public int hashCode() {
		return String.valueOf(time).hashCode() * 37;
	}

	@Override
	public Pos clone() throws CloneNotSupportedException {
		return (Pos) super.clone();
	}
}
