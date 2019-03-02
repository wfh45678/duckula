package net.wicp.tams.duckula.dump.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import net.wicp.tams.common.metrics.core.TsMetricAbstractGroup;

public class DumpGroup extends TsMetricAbstractGroup {
	public DumpGroup(String serviceName) {
		super(serviceName);
	}

	// 所有包
	public final Meter meter_read_db = newMeter(this.getClass(), "meter_read_db");

	public final Meter meter_ringbuff = newMeter(this.getClass(), "meter_ringbuff");

	// public final Meter meter_busi_do = newMeter(this.getClass(),
	// "meter_busi_do");

	// public final Meter meter_send_es = newMeter(this.getClass(),
	// "meter_send_es");

	// public final Meter meter_send_error = newMeter(this.getClass(),
	// "meter_send_error");

	public final Counter counter_read_db = newCounter(this.getClass(), "counter_read_db");

	public final Counter counter_busi_do = newCounter(this.getClass(), "counter_busi_do");
	// 发送的事件数，
	public final Counter counter_send_event = newCounter(this.getClass(), "counter_send_event");
	// 发送的记录数
	public final Counter counter_send_es = newCounter(this.getClass(), "counter_send_es");
	// 失败的记录数
	public final Counter counter_send_error = newCounter(this.getClass(), "counter_send_error");

	public final Gauge<Integer> guage_read_db = newGauge(new Gauge<Integer>() {
		@Override
		public Integer getValue() {
			return (int) meter_read_db.getCount();
		}
	}, this.getClass(), "guage_read_db");

	public final Gauge<Integer> guage_ringbuff = newGauge(new Gauge<Integer>() {
		@Override
		public Integer getValue() {
			return (int) meter_ringbuff.getCount();
		}
	}, this.getClass(), "guage_ringbuff");

}
