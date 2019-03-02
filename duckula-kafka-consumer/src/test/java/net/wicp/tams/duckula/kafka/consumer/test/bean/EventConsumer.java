package net.wicp.tams.duckula.kafka.consumer.test.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventConsumer {
	private Long data;
}
