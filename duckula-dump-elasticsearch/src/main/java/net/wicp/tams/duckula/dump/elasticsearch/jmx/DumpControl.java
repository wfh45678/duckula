package net.wicp.tams.duckula.dump.elasticsearch.jmx;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.constant.JvmStatus;

@Data
@Slf4j
public class DumpControl implements DumpControlMBean {

	@Override
	public void stop() {
		log.info("通过MBean服务停止服务");
		// TODO 不在同一线程，肯定出错
		/*
		 * try { lock.release(); } catch (Exception e) { log.error("解锁失败", e); }
		 */
		LoggerUtil.exit(JvmStatus.s15);
	}

}
