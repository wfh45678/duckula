package net.wicp.tams.duckula.ops.beans;

import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.beans.MemoryInfo;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;

@Data
@Slf4j
public class Server implements Comparable<Server> {
	private String ip;
	private String name;
	private YesOrNo isInit = YesOrNo.no;
	private final int serverPort = 22;//
	private String lockIp;// 用于查在分布式锁上用的IP。默认与ip相同，也有可能不同
	private String remark;
	private Boolean run;// ops为构造那构运行的树
	private YesOrNo useDocker = YesOrNo.no;// 是否使用docker
	private String syncConfDate;//配置文件同步时间

	private MemoryInfo mi;

	public List<String> findTasks() {
		List<String> tasks = ZkClient.getInst().getChildren(ZkPath.servers.getRoot());
		return tasks;
	}

/*	public ServerCommon findServerCommon() {
		ServerCommon serverCommon = ZkClient.getInst().getDateObj(ZkPath.servers.getRoot(), ServerCommon.class);
		return serverCommon;
	}*/

	@Override
	public int compareTo(Server s) {
		if (this.mi == null || s.mi == null) {
			return 0;
		}
		long def = this.mi.getFreeMemory() - s.getMi().getFreeMemory();
		return def > 0 ? 1 : -1;
	}
	
	public static void packageResources(List<Server> retlist) {
		for (Server server : retlist) {
			SSHConnection conn = null;
			try {
				conn = DuckulaUtils.getConn(server);
				MemoryInfo freeM = conn.freeM();
				server.setMi(freeM);
			} catch (Exception e) {
				log.error("查询服务器" + server.getIp() + "监控信息失败", e);
			} finally {
				if (conn != null) {
					DuckulaUtils.returnConn(server, conn);
				}
			}
		}
	}
}
