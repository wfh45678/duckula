package net.wicp.tams.duckula.ops.servicesBusi;

import java.util.List;
import java.util.Map;

import net.wicp.tams.common.Result;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.beans.PosShow;
import net.wicp.tams.duckula.ops.beans.Server;

public interface IDuckulaAssit {

	// public static final String urlFormat =
	// "http://%s:%s/duckula-server/connector";

	// public static final String plugQueryFormat =
	// "http://%s:%s/duckula-server/web/plugins";

	// public static final String plugUploadFormat =
	// "http://%s:%s/duckula-server/web/upload";

	public List<Server> findAllServers();

	public List<DbInstance> findAllDbInstances();

	public List<Task> findAllTasks();

	public List<PosShow> findAllPosForTasks();

	public Map<String, Map<ZkPath, List<String>>> serverRunTaskDetail(List<Server> servers);

	public Map<String, Integer> serverRunTaskNum(List<Server> servers);

	public List<String> lockToServer(List<Server> findAllServers, ZkPath zkPath, String taskId);// 把锁值里面显示的IP转成对应的Server
																								// IP

	/***
	 * 找到内存最大的机器
	 */
	public Server selServer(String... removeIps);

	public Result startTask(CommandType commandType, String taskId, Server server, boolean isAuto);

	// k8s启动任务
	/***
	 * 
	 * @param commandType
	 * @param taskId
	 * 
	 * @return
	 */
	public Result startTaskForK8s(CommandType commandType, String taskId, boolean isAuto);

	public Result stopTask(CommandType commandType, String taskId, Server server, boolean isAuto);

	// 停止k8s服务
	public Result stopTaskForK8s(CommandType commandType, String taskId, boolean isAuto);

	// 重新启动服务
	public void reStartTask(CommandType commandType, String childrenId, String... removeIps);
}
