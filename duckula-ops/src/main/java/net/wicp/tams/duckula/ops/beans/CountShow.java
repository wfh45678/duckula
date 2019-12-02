package net.wicp.tams.duckula.ops.beans;

import lombok.Data;

@Data
public class CountShow {
	private String id;
	private long insertNum;
	private long updateNum;
	private long deleteNum;
	private long filterNum;

	private long allPack;

	private long parserPack;
	private long parserEvent;
	private long sendEvent;

	private long ringbuffPack;
	private long ringbuffEvent;

	// 速度
	private long dowithNum;
	private String meanRate;
	private String oneMinuteRate;
	private String fiveMinuteRate;
	private String fifteenMinuteRate;

	// ringbuff变量
	private long undoSize;
	private long senderUnit;
}
