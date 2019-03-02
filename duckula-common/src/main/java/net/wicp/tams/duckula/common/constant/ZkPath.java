package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.Conf;
import net.wicp.tams.duckula.common.beans.ColHis;

public enum ZkPath {
	tasks("任务路径"),

	tasksofflines("离线任务路径"),

	pos("位置路径"),

	counts("计数器路径"),

	dbinsts("数据库实例配置"),

	servers("执行服务器配置"),

	cols("列名的路径"),

	mappings("es的索引mapping"),

	dumps("全量导入处理任务"),

	consumers("kafka监听任务"),

	busiplugins("业务插件");

	private final String desc;
	// private final String path;

	private ZkPath(String desc) {
		this.desc = desc;
	}

	public String getDesc() {
		return desc;
	}

	public String getRoot() {
		return String.format("%s/%s", Conf.get("duckula.zk.rootpath"), this.name());
		// return this.path;
	}

	public String getPath(String taskId) {
		return String.format("%s/%s", getRoot(), taskId);
	}

	public String getColsPath(String taskId, ColHis colhis) {
		if (this != ZkPath.cols) {
			throw new IllegalAccessError("	只有列名的路径能访问此方法");
		}
		return String.format("%s/%s/%s|%s", getRoot(), taskId, colhis.getDb(), colhis.getTb());
	}

	public static ZkPath getByName(String zkPath) {
		for (ZkPath ele : ZkPath.values()) {
			if (ele.name().equalsIgnoreCase(zkPath)) {
				return ele;
			}
		}
		return null;
	}
}
