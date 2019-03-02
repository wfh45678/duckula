package net.wicp.tams.duckula.task.jmx;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.common.beans.Count;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.conf.ZookeeperImpl;

@Data
@Slf4j
public class BinlogControl implements BinlogControlMBean {
	private final InterProcessMutex lock;
	private final ZookeeperImpl taskConf;

	public BinlogControl(InterProcessMutex lock, ZookeeperImpl taskConf) {
		this.lock = lock;
		this.taskConf = taskConf;
	}

	@Override
	public void stop() {
		log.info("通过MBean服务停止服务");
		try {
			lock.release();
		} catch (Exception e) {
			log.error("解锁失败", e);
		}
		LoggerUtil.exit(JvmStatus.s15);
	}

	@Override
	public void putSync(boolean isSync) {
		Main.context.setSync(isSync);
	}

	@Override
	public short getSyncStatus() {
		return Main.context.isSync() ? (short) 1 : (short) 0;
	}

	// long类型也不支持export，只支持int和double ,int能支持68年，到2038
	@Override
	public int getCurPos() {
		return new Long(taskConf.getCurPos().getTime()).intValue();
	}

	@Override
	public Count getCount() {
		return taskConf.getCurCount();
	}

}
