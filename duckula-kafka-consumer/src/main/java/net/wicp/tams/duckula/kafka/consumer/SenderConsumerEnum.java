package net.wicp.tams.duckula.kafka.consumer;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
import net.wicp.tams.duckula.common.constant.SenderEnum;

public enum SenderConsumerEnum implements IEnumCombobox {

	es("es搜索", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerEsImpl"),

	jdbc("mysql", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerMysqlImpl");

	private final String desc;
	private final String pluginClass;// 值

	public String getPluginClass() {
		return pluginClass;
	}

	private SenderConsumerEnum(String desc, String pluginClass) {
		this.desc = desc;
		this.pluginClass = pluginClass;
	}

	public static SenderEnum get(String name) {
		for (SenderEnum senderEnum : SenderEnum.values()) {
			if (senderEnum.name().equalsIgnoreCase(name)) {
				return senderEnum;
			}
		}
		return null;
	}

	public String getDesc() {
		return desc;
	}

	@Override
	public String getName() {
		return this.name();
	}

	@Override
	public String getDesc_en() {
		return this.desc;
	}

	@Override
	public String getDesc_zh() {
		return this.desc;
	}

}
