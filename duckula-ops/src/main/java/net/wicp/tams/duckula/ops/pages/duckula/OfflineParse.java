package net.wicp.tams.duckula.ops.pages.duckula;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.callback.impl.convertvalue.ConvertValueDate;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.thread.ThreadPool;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.common.constant.ZkPath;

@Slf4j
public class OfflineParse {

	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() throws KeeperException, InterruptedException {
		final TaskOffline taskOffline = TapestryAssist.getBeanFromPage(TaskOffline.class, requestGlobals);

		List<String> offlineTaskIds = ZkClient.getInst().getChildren(ZkPath.tasksofflines.getRoot());
		List<TaskOffline> tasks = CollectionFactory.newList();
		for (String nodeName : offlineTaskIds) {
			TaskOffline buidlTaskOffline = ZkUtil.buidlTaskOffline(nodeName);
			tasks.add(buidlTaskOffline);
		}

		List<TaskOffline> retlist = (List<TaskOffline>) CollectionUtils.select(tasks, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				TaskOffline temp = (TaskOffline) object;
				boolean ret = true;
				if (StringUtil.isNotNull(taskOffline.getId())) {
					ret = temp.getId().indexOf(taskOffline.getId()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}

		});

		ConvertValueDate convertValueDate = new ConvertValueDate(DateFormatCase.YYYY_MM_DD_hhmmss);

		String retstr = EasyUiAssist.getJsonForGrid(retlist,
				new String[] { "id", "taskOnline.id,taskOnlineId", "runStartTime", "runEndTime", "gtidBegin", "gtidEnd",
						"binlogFiles", "timeBegin", "timeEnd", "taskOnline.dbinst,dbinst" },
				new IConvertValue[] { null, null, null, null, null, null, null, convertValueDate, convertValueDate,
						null },
				retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onDel() throws KeeperException, InterruptedException {
		final TaskOffline taskparam = TapestryAssist.getBeanFromPage(TaskOffline.class, requestGlobals);
		Result ret = ZkUtil.del(ZkPath.tasksofflines, taskparam.getId());
		return TapestryAssist.getTextStreamResponse(ret);
	}

	//TODO [Curator-Framework-0] State change: SUSPENDED   在cmd执行没问题
	public TextStreamResponse onRuntask() throws KeeperException, InterruptedException, IOException {
		String offId = request.getParameter("id");
		// TaskOffline param = ZkUtil.buidlTaskOffline(offId);
		String logDir = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/logs/taskOffline", offId);
		String cmdtrue = String.format(
				"java -DlogRoot=" + logDir + " -cp %s/duckula-task.jar net.wicp.tams.duckula.task.MainOffline %s",
				System.getenv("DUCKULA_HOME"), offId);
		log.info("start task:{}", offId);
		try {
			final Process ps = Runtime.getRuntime().exec(cmdtrue);
			ps.waitFor(1, TimeUnit.SECONDS);
			Future<String> query = (Future<String>) ThreadPool.getDefaultPool().submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					String errstr = IOUtil.slurp(ps.getErrorStream(), Conf.getSystemEncode());
					return errstr;
				}
			});
			try {
				String retstr = query.get(1, TimeUnit.SECONDS);
				if (StringUtil.isNotNull(retstr)) {
					return req.retErrorInfo(retstr);
				} else {
					return req.retSuccInfo("已提交任务，执行结果请查看相关日志");
				}
			} catch (ExecutionException e) {
				log.info("超时获得错误流，意味着命令没问题");
				return req.retErrorInfo("超时获得错误流，意味着命令没问题");
			} catch (TimeoutException e) {
				log.info("超时获得错误流，意味着命令没问题");
				return req.retErrorInfo("超时获得错误流，意味着命令没问题");
			}
		} catch (IOException ioe) {
			log.error("IO异常，文件有误", ioe);
			return req.retErrorInfo("IO异常，文件有误");
		} catch (InterruptedException e) {
			log.error("中断异常", e);
			return req.retErrorInfo("中断异常");
		}
	}

}
