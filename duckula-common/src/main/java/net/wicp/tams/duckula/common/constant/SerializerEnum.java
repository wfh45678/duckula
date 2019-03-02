package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum SerializerEnum implements IEnumCombobox {
	protobuff3("protobuff3", "/serializer/duckula-serializer-protobuf3/"),

	protobuff2("protobuff2", "/serializer/duckula-serializer-protobuf2/"),

	thrift("thrift", "/serializer/duckula-serializer-thrift/"),

	no("不序列化", "");

	private final String desc;
	private final String pluginJar;// 值

	public String getPluginJar() {
		// String pathStr =
		// IOUtil.mergeFolderAndFilePath(SenderEnum.rootDir.getPath(),
		// this.pluginJar);
		return pluginJar;
	}

	private SerializerEnum(String desc, String pluginJar) {
		this.desc = desc;
		this.pluginJar = pluginJar;
	}

	public static SerializerEnum get(String name) {
		for (SerializerEnum serialEnum : SerializerEnum.values()) {
			if (serialEnum.name().equalsIgnoreCase(name)) {
				return serialEnum;
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
