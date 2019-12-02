package net.wicp.tams.duckula.common.beans;

import lombok.Builder;
import lombok.Data;

/***
 * 统计数据
 * 
 * @author zhoujunhui
 *
 */
@Data
@Builder
public class Count {
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

	public Count() {
	}

	public Count(long insertNum, long updateNum, long deleteNum, long filterNum, long allPack, long parserPack,
			long parserEvent, long sendEvent, long ringbuffPack, long ringbuffEvent, long dowithNum, String meanRate,
			String oneMinuteRate, String fiveMinuteRate, String fifteenMinuteRate, long undoSize, long senderUnit) {
		super();
		this.insertNum = insertNum;
		this.updateNum = updateNum;
		this.deleteNum = deleteNum;
		this.filterNum = filterNum;
		this.allPack = allPack;
		this.parserPack = parserPack;
		this.parserEvent = parserEvent;
		this.sendEvent = sendEvent;
		this.ringbuffPack = ringbuffPack;
		this.ringbuffEvent = ringbuffEvent;
		this.meanRate = meanRate;
		this.oneMinuteRate = oneMinuteRate;
		this.fiveMinuteRate = fiveMinuteRate;
		this.fifteenMinuteRate = fifteenMinuteRate;
		this.undoSize = undoSize;
		this.senderUnit = senderUnit;
		this.dowithNum = dowithNum;
	}

}
