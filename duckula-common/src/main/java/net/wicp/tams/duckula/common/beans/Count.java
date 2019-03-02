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

	private long allPack;

	private long parserPack;
	private long parserEvent;
	private long sendEvent;

	private long ringbuffPack;
	private long ringbuffEvent;

	public Count() {
	}

	public Count(long insertNum, long updateNum, long deleteNum, long allPack, long parserPack, long parserEvent,
			long sendEvent, long ringbuffPack, long ringbuffEvent) {
		super();
		this.insertNum = insertNum;
		this.updateNum = updateNum;
		this.deleteNum = deleteNum;
		this.allPack = allPack;
		this.parserPack = parserPack;
		this.parserEvent = parserEvent;
		this.sendEvent = sendEvent;
		this.ringbuffPack = ringbuffPack;
		this.ringbuffEvent = ringbuffEvent;
	}

}
