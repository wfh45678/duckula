package net.wicp.tams.duckula.task.conf;

import java.util.Map;
import java.util.SortedSet;

import net.wicp.tams.duckula.common.beans.ColHis;
import net.wicp.tams.duckula.common.beans.Count;
import net.wicp.tams.duckula.common.beans.Pos;

public interface ITaskConf {
	/**
	 * 创建task信息
	 */
	public void buildTask();

	/***
	 * 得到位点信息
	 */
	public void buildPos();
	

	/***
	 * 更新位点信息
	 * 
	 */
	public void updatePos(Pos pos);

	/***
	 * 更新计数信息
	 * 
	 */
	public void updateCount(Count count);

	/***
	 * 检查pos是否正常：zk没有相应节点或 有节点
	 * 
	 * @return
	 */
	public boolean checkHasPos();

	/***
	 * 初始化配置信息
	 */
	public void init(String taskId);

	/***
	 * 得到cols的历史信息
	 * 
	 * @return
	 */
	public Map<String, SortedSet<ColHis>> findCols();
}
