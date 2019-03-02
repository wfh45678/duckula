package net.wicp.tams.duckula.ops.servicesBusi;

public enum Command {
	TaskStart("开启Task"),
	
	TaskStop("停止Task"),
	
	MonitorShow("显示监控");
	
	private final String desc;
	
	public String getDesc() {
		return desc;
	}

	private Command(String desc) {
		this.desc=desc;
	}
}
