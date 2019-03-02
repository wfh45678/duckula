package net.wicp.tams.duckula.task.constant;

public enum Checksum {
	CRC32("CRC32较验", 4), NONE("不用较验", 0);

	private final String desc;
	private final int byteNum;// 检查字节数

	private Checksum(String desc, int byteNum) {
		this.desc = desc;
		this.byteNum = byteNum;
	}

	public static Checksum get(String name) {
		for (Checksum checksum : Checksum.values()) {
			if (checksum.name().equalsIgnoreCase(name)) {
				return checksum;
			}
		}
		return NONE;
	}

	public String getDesc() {
		return desc;
	}

	public int getByteNum() {
		return byteNum;
	}
}
