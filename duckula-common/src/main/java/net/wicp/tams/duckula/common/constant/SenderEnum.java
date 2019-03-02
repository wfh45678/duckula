package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum SenderEnum implements IEnumCombobox {
	// log("日志服务", "net.wicp.tams.commons.binlog.sender.impl.LogFile"),

	kafka("kafka消息", "/sender/duckula-plugin-kafka/", MiddlewareType.kafka),

	es("es搜索", "/sender/duckula-plugin-elasticsearch/", MiddlewareType.es),

	redis("redis缓存", "/sender/duckula-plugin-redis/", MiddlewareType.redis),

	// ons("ons消息", "net.wicp.tams.commons.binlog.sender.impl.SenderOns"),

	no("其它发送者", "", null);

	private final String desc;
	private final String pluginJar;// 值
	private final MiddlewareType middlewareType;// 关联的中间件类型

	public String getPluginJar() {
		// String pathStr = IOUtil.mergeFolderAndFilePath(rootDir.getPath(),
		// this.pluginJar);
		return pluginJar;
	}

	private SenderEnum(String desc, String pluginJar, MiddlewareType middlewareType) {
		this.desc = desc;
		this.pluginJar = pluginJar;
		this.middlewareType = middlewareType;
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

	public MiddlewareType getMiddlewareType() {
		return middlewareType;
	}
}
