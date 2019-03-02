package net.wicp.tams.duckula.task.jmx;

import net.wicp.tams.duckula.common.beans.Count;

public interface BinlogControlMBean {

	/***
	 * 停止进程
	 */
	public void stop();

	/***
	 * 设置是否同步
	 * 
	 * @param isSync
	 *            true:是 false:否
	 */
	public void putSync(boolean isSync);

	/**
	 * 查看是否同步,1:是同步,0:异步
	 * 
	 * @return
	 */
	public short getSyncStatus();

	public int getCurPos();

	public Count getCount();
}
