package net.wicp.tams.duckula.task.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GtidBean {
	private String gtids;
	private long commitTime;
}
