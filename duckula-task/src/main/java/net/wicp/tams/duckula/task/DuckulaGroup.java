package net.wicp.tams.duckula.task;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import net.wicp.tams.common.metrics.core.TsMetricAbstractGroup;

public class DuckulaGroup extends TsMetricAbstractGroup {
	public DuckulaGroup(String serviceName) {
		super(serviceName);
	}

	// 所有包
	public final Meter meter_parser_pack_all = newMeter(this.getClass(), "meter_parser_pack_all");
	// 有效的包
	public final Meter meter_parser_pack_row = newMeter(this.getClass(), "meter_parser_pack_sel");
	// 记录解析
	public final Meter meter_parser_event = newMeter(this.getClass(), "meter_parser_pack");

	// 包发送
	public final Meter meter_sender_pack = newMeter(this.getClass(), "meter_sender_pack");
	// 记录发送
	public final Meter meter_sender_event = newMeter(this.getClass(), "meter_sender_event");

	// 新增记录数
	public final Meter meter_sender_event_add = newMeter(this.getClass(), "meter_sender_event_add");
	
	// 过滤记录数
	public final Meter meter_sender_event_filter = newMeter(this.getClass(), "meter_sender_event_filter");

	// 删除记录数
	public final Meter meter_sender_event_del = newMeter(this.getClass(), "meter_sender_event_del");

	// 修改记录数
	public final Meter meter_sender_event_update = newMeter(this.getClass(), "meter_sender_event_update");
	
	// 处理记录记录数
    public final Meter meter_dowith_event = newMeter(this.getClass(), "meter_dowith_event");

	public final Counter counter_ringbuff_pack = newCounter(this.getClass(), "counter_ringbuff_pack");

	public final Counter counter_ringbuff_event = newCounter(this.getClass(), "counter_ringbuff_event");

	public final Gauge<Integer> guage_ringbuff_event = newGauge(new Gauge<Integer>() {
		@Override
		public Integer getValue() {
			return (int) counter_ringbuff_event.getCount();
		}
	}, this.getClass(), "guage_ringbuff_event");

	public final Gauge<Integer> guage_ringbuff_pack = newGauge(new Gauge<Integer>() {
		@Override
		public Integer getValue() {
			return (int) counter_ringbuff_pack.getCount();
		}
	}, this.getClass(), "guage_ringbuff_pack");

}
