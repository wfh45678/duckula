package net.wicp.tams.duckula.kafka.consumer.test.handlerConsumer;

import com.lmax.disruptor.WorkHandler;

import net.wicp.tams.duckula.kafka.consumer.test.bean.EventConsumer;

public class BusiHander implements WorkHandler<EventConsumer> {

	@Override
	public void onEvent(final EventConsumer event) throws Exception {
		System.out.println("busi=" + event.getData().longValue());
		return;
	}

}
