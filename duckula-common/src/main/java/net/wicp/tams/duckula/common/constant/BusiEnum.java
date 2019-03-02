package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum BusiEnum implements IEnumCombobox {
	no("不处理", ""), // 默认选项

	filter("过滤插件", "/busi/duckula-busi-filter/"),

	custom("自定义", "");

	private final String desc;
	private final String pluginJar;// 值

	public String getPluginJar() {
		// String pathStr =
		// IOUtil.mergeFolderAndFilePath(SenderEnum.rootDir.getPath(),
		// this.pluginJar);
		return pluginJar;
	}

	private BusiEnum(String desc, String pluginJar) {
		this.desc = desc;
		this.pluginJar = pluginJar;
	}

	public static BusiEnum get(String name) {
		for (BusiEnum busiEnum : BusiEnum.values()) {
			if (busiEnum.name().equalsIgnoreCase(name)) {
				return busiEnum;
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
