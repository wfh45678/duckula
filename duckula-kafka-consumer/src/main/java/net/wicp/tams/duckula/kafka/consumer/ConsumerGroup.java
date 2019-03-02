package net.wicp.tams.duckula.kafka.consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import net.wicp.tams.common.metrics.core.TsMetricAbstractGroup;

public class ConsumerGroup extends TsMetricAbstractGroup {
	public ConsumerGroup(String serviceName) {
		super(serviceName);
	}

	// 所有包
	public final Meter meter_read_kafka = newMeter(this.getClass(), "meter_read_kafka");

	public final Counter counter_read_kafka = newCounter(this.getClass(), "counter_read_kafka");
	// 发送的记录数
	public final Counter counter_send_es = newCounter(this.getClass(), "counter_send_es");

	// 发送的批次数
	public final Counter counter_send_batch = newCounter(this.getClass(), "counter_send_batch");

	// 失败的记录数
	public final Counter counter_send_error = newCounter(this.getClass(), "counter_send_error");

	public final Gauge<Integer> guage_read_kafka = newGauge(new Gauge<Integer>() {
		@Override
		public Integer getValue() {
			return (int) meter_read_kafka.getCount();
		}
	}, this.getClass(), "guage_read_kafka");

}
