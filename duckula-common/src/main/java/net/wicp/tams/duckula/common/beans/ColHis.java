package net.wicp.tams.duckula.common.beans;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.wicp.tams.common.apiext.StringUtil;

@Getter
@Setter
@ToString
public class ColHis implements Comparable<ColHis> {
	private String db;
	private String tb;
	private long time;
	private String[] cols;
	private String[] colTypes;// 添加 colTypes，为创建mapping做准备

	@Override
	public boolean equals(Object obj) {
		ColHis temp = (ColHis) obj;
		return this.time == temp.getTime();
	}

	@Override
	public int hashCode() {
		return StringUtil.hashInt(String.valueOf(time));
	}

	@Override
	public int compareTo(ColHis o) {
		long def = o.getTime() - time;
		return def > 0 ? 1 : (def < 0 ? -1 : 0);
	}

}
