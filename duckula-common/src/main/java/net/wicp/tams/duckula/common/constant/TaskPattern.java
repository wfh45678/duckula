package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

/*****
 * task启动模式
 * 
 * @author andy.zhou
 *
 */
public enum TaskPattern implements IEnumCombobox {
	process("进程或docker", new String[] {}), // 默认选项

	k8s("k8s", new String[] { "servermanager" }),

	tiller("chart启动", new String[] { "servermanager" });

	private final String desc;

	private final String[] excludeMenu;// 不希望出现的菜单

	/***
	 * 是否需要server
	 * 
	 * @return
	 */
	public static boolean isNeedServer() {
		TaskPattern curpattern = getCurTaskPattern();
		return curpattern == process ? true : false;
	}

	public static TaskPattern getCurTaskPattern() {
		String patternStr = Conf.get("duckula.ops.starttask.pattern");
		TaskPattern curpattern = TaskPattern.get(patternStr);
		return curpattern;
	}

	private TaskPattern(String desc, String[] excludeMenu) {
		this.desc = desc;
		this.excludeMenu = excludeMenu;
	}

	public static TaskPattern get(String name) {
		if (StringUtil.isNull(name)) {
			return process;
		}
		for (TaskPattern busiEnum : TaskPattern.values()) {
			if (busiEnum.name().equalsIgnoreCase(name)) {
				return busiEnum;
			}
		}
		return process;
	}

	public String getDesc() {
		return desc;
	}

	public String[] getExcludeMenu() {
		return excludeMenu;
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
