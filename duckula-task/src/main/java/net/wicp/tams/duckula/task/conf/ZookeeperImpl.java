package net.wicp.tams.duckula.task.conf;

import java.util.Map;
import java.util.SortedSet;

import org.apache.zookeeper.data.Stat;

import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.ColHis;
import net.wicp.tams.duckula.common.beans.Count;
import net.wicp.tams.duckula.common.beans.Pos;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.task.Main;

public class ZookeeperImpl extends AbsTaskConf {

	private Pos curPos;

	private Count curCount;

	@Override
	public void initSub() {
		// 创建task根目录
		for (ZkPath zkPath : ZkPath.values()) {
			Stat stat = ZkUtil.exists(zkPath);
			if (stat == null) {
				ZkClient.getInst().createMultilevelNode(zkPath.getRoot());
			}
		}
	}

	@Override
	public void buildTask() {
		Stat stat = ZkUtil.exists(ZkPath.tasks, taskId);
		if (stat == null) {
			throw new RuntimeException("没有此任务：" + taskId + "，请配置。");
		}
		Main.context.setTask(ZkUtil.buidlTask(taskId));
	}

	@Override
	public void buildPos() {
		if (Main.context.getPos() == null) {
			if (Main.context.getTask() == null) {
				buildTask();
			}
			Stat posStat = ZkUtil.exists(ZkPath.pos, taskId);
			Pos dbPos = getMastStatus(Main.context.getTask());// 现在运行的位点
			if (posStat != null && posStat.getDataLength() > 0) {
				// 存在此节点
				Pos posZk = ZkUtil.buidlPos(taskId);
				if (dbPos.getMasterServerId() != posZk.getMasterServerId()) {// 做了主备切换
					dbPos.setGtids(posZk.getGtids());// 做了主备时，gtid与zk保持一致，但pos需要用新服务器的pos
					Main.context.setParsePos(dbPos);
					try {
						Main.context.setInitPos(dbPos.clone());
					} catch (CloneNotSupportedException e) {
						throw new RuntimeException("主备时不能clone初始位点");
					}
				} else {
					Main.context.setParsePos(posZk);
				}
			} else {
				Main.context.setParsePos(dbPos);
			}
		}
	}
	


	@Override
	public void updatePos(Pos pos) {
		ZkUtil.updatePos(taskId, pos);
		this.curPos = pos;
	}

	@Override
	public void updateCount(Count count) {
		ZkUtil.updateCount(taskId, count);
		this.curCount = count;
	}

	@Override
	public Map<String, SortedSet<ColHis>> findCols() {
		Stat colsStat = ZkUtil.exists(ZkPath.cols, taskId);
		if (colsStat == null) {
		} else {
		}
		return null;
	}

	@Override
	public boolean checkHasPos() {
		Stat posStat = ZkUtil.exists(ZkPath.pos, taskId);
		return posStat != null;
	}

	public Pos getCurPos() {
		return curPos;
	}

	public Count getCurCount() {
		return curCount;
	}

	

}
