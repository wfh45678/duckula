package net.wicp.tams.duckula.common.beans;

import java.util.Date;

import lombok.Data;

/***
 * 离线解析任务
 *
 * @author zhoujunhui
 *
 */
@Data
public class TaskOffline {
	private String id;
	private String taskOnlineId;
	private Date timeBegin;
	private Date timeEnd;
	private String gtidBegin;
	private String gtidEnd;
	private String binlogFiles;
	private String dbId;//日志所在域ID // 日志所在rds主机Id,为数字，task的dbinst是rds实例名，为字符串
	private long posBegin;//指定从哪个位置开始读。默认的离线任务不会用它

	private int limitType = 1;// 限制类型，1:时间 2：gtid 默认为1
	private Task taskOnline;// 需要跟据taskOnlineId得到在线Task
	// 由于可能出现多套库，多套表，所以。col需要在跟据db/tb在zk上查询
	
    private String runStartTime;
    private String runEndTime;
}
