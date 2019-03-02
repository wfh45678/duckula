package net.wicp.tams.duckula.ops.beans;

import lombok.Data;

/***
 * 放到sesseion中的数据 尽量不用sesseion，否则会不方便做扩展
 * 
 * @author 偏锋书生
 *
 *         2018年8月27日
 */
@Data
public class Session {
	private String cluster;
}
