package net.wicp.tams.duckula.busi.filter;

public enum Pattern {
	regular("正则表达式"), sql("带参数SQL"), function("自定义函数，考虑使用脚本语言");

	private final String desc;


	public String getDesc() {
		return desc;
	}

	private Pattern(String desc) {
		this.desc = desc;
	}
}
