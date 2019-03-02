package net.wicp.tams.duckula.kafka.consumer.test.handlerConsumer;

import com.lmax.disruptor.WorkHandler;

import net.wicp.tams.duckula.kafka.consumer.test.bean.EventConsumer;

public class SendHander implements WorkHandler<EventConsumer> {

	@Override
	public void onEvent(EventConsumer event) throws Exception {
		System.out.println("send="+event.getData().longValue());

	}

}
