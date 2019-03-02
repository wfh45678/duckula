package net.wicp.tams.duckula.task.bean;

import lombok.Data;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;

@Data
public class EventPackage extends DuckulaPackage {
	//private boolean isXid;// 事务是否结束，xid事件发生即结束
	private long xid;// 事务是否结束，xid事件发生即结束
	private Rule rule;// 规则
	private int[] partitions;
	private Pos pos;
	private boolean isOver;// 是否结束，如果为true，后续不需要处理此包。默认为否，
}
