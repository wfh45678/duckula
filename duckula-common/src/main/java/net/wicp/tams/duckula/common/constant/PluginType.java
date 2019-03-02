package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum PluginType implements IEnumCombobox {
	consumer("kafka消费插件", "/consumers"),

	serializer("序列化插件", "/serializer"),

	sender("发送者插件", "/sender");

	private final String desc;
	private final String pluginDir;// 值

	public String getPluginDir(boolean isconfig) {
		String datadir =isconfig? Conf.get("duckula.ops.datadir"):System.getenv("DUCKULA_DATA");
		String pluginRoot = IOUtil.mergeFolderAndFilePath(datadir, pluginDir);
		return pluginRoot;
	}

	private PluginType(String desc, String pluginDir) {
		this.desc = desc;
		this.pluginDir = pluginDir;
	}

	public static PluginType get(String name) {
		for (PluginType pluginType : PluginType.values()) {
			if (pluginType.name().equalsIgnoreCase(name)) {
				return pluginType;
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
