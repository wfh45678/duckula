package net.wicp.tams.duckula.ops.pages.duckula;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FileUtils;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse.NodeInfo;

import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.DateUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.TarUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.callback.impl.convertvalue.ConvertValueSize;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.SizeUnit;
import net.wicp.tams.common.others.RdsUtil;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.beans.TaskOffline;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;

public class BinlogDownload {

	protected static final Date changeTimeZone = null;

	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;
	@Inject
	private IReq req;

	public TextStreamResponse onQuery() throws KeeperException, InterruptedException, ParseException {
		String dbId = request.getParameter("dbId");
		String hostId1 = request.getParameter("hostId1");
		String hostId2 = request.getParameter("hostId2");
		String hostId3 = request.getParameter("hostId3");

		String beginDate = request.getParameter("beginDate");
		String endDate = request.getParameter("endDate");
		if (StringUtil.isNull(dbId) || StringUtil.isNull(hostId1) || StringUtil.isNull(beginDate)) {
			return TapestryAssist.getTextStreamResponse(EasyUiAssist.getJsonForGridEmpty());
		}
		String[] hostIds = new String[] { hostId1, hostId2, hostId3 };
		Date startTime = DateFormatCase.YYYY_MM_DD.getInstanc().parse(beginDate);
		Date endTime = StringUtil.isNull(endDate) ? new Date() : DateFormatCase.YYYY_MM_DD.getInstanc().parse(endDate);
		List<BinLogFile> binlogfiles = RdsUtil.findBinLogFilesMax(startTime, endTime, dbId, hostIds);

		IConvertValue<Object> fileconv = new IConvertValue<Object>() {
			@Override
			public String getStr(Object keyObj) {
				BinLogFile binLogFile = (BinLogFile) keyObj;
				String filename = String.format("%s/%s", binLogFile.getHostInstanceID(),
						StringUtil.getFileName(binLogFile.getDownloadLink()));
				return filename;
			}
		};
		@SuppressWarnings("rawtypes")
		Map<String, IConvertValue> conmap = new HashMap<>();
		conmap.put("filename", fileconv);
		conmap.put("fileSize1", new ConvertValueSize(SizeUnit.B, SizeUnit.MB));

		final List<String> fileNames = new ArrayList<>();
		for (String hostId : hostIds) {
			if (StringUtil.isNull(hostId)) {
				continue;
			}
			String logDir = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/binlog", dbId, hostId);
			File dir = new File(logDir);
			if (dir.exists()) {
				File[] files = dir.listFiles();
				for (File file : files) {
					fileNames.add(String.format("%s:%s", hostId, file.getName()));
				}
			}
		}

		// 文件是否存在
		IConvertValue<String> fileExit = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				// http://rdslog-hzi-v2.oss-cn-hangzhou-i.aliyuncs.com/custins1791887/hostins3257251/mysql-bin.001120.tar?OSSAccessKeyId=LTAITfQ7krsrEwRn&Expires=1528785359&Signature=mqxwBsM5pSeMNLZGlU6X%2FC8Gwz8%3D
				int begin = keyObj.indexOf("/hostins");
				int end = keyObj.indexOf("/", begin + 1);
				String hostId = keyObj.substring(begin + 8, end);
				String filename_tar = StringUtil.getFileName(keyObj);
				String filename = filename_tar.replace(".tar", "");
				int retint = 0;
				if (fileNames.contains(String.format("%s:%s", hostId, filename))) {
					retint += 2;
				}
				if (fileNames.contains(String.format("%s:%s", hostId, filename_tar))) {
					retint += 1;
				}
				return String.valueOf(retint);
			}
		};
		conmap.put("fileexit", fileExit);
		IConvertValue<String> dateConvert = new IConvertValue<String>() {

			@Override
			public String getStr(String keyObj) {
				try {
					Date parse = DateFormatCase.TZyyyyMMddHHmmss.getInstanc().parse(keyObj);
					Date changeTimeZone = DateUtil.changeTimeZone(parse, DateUtil.getTZZone(),
							DateUtil.getBeijingTimeZone());
					String retstr = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(changeTimeZone);
					return retstr;
				} catch (ParseException e) {
					return "error:" + keyObj;
				}
			}
		};
		conmap.put("logBeginTime2", dateConvert);
		conmap.put("logEndTime2", dateConvert);

		String retstr = EasyUiAssist.getJsonForGridAlias2(
				binlogfiles, new String[] { ",filename", "downloadLink,fileexit", "fileSize,fileSize1",
						"hostInstanceID,hostId", "logBeginTime,logBeginTime2", "logEndTime,logEndTime2" },
				conmap, (long) binlogfiles.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onQueryDbInsts() throws KeeperException, InterruptedException {
		List<String> dbs = ZkClient.getInst().getChildren(ZkPath.dbinsts.getRoot());
		String retstr = JSONUtil.getJsonForListSimple(dbs);
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onDownloadFile() throws KeeperException, InterruptedException, IOException {
		BinLogFile binLogFile = TapestryAssist.getBeanFromPage(BinLogFile.class, request);
		String dbId = request.getParameter("dbId");
		String logDirPath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/binlog", dbId,
				binLogFile.getHostInstanceID());
		File file = new File(logDirPath);
		if (!file.exists()) {
			FileUtils.forceMkdir(file);
		}
		String str = RdsUtil.downloadBinLogFiles(binLogFile, logDirPath);
		TarUtil.decompress(str);
		return TapestryAssist.getTextStreamResponse(Result.getSuc(str));
	}

	public TextStreamResponse onDelTarAndLog() throws KeeperException, InterruptedException, IOException {
		BinLogFile binLogFile = TapestryAssist.getBeanFromPage(BinLogFile.class, request);
		String filename_tar = request.getParameter("filename");
		String filename = filename_tar.replace(".tar", "");

		String logDir = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/binlog",
				binLogFile.getHostInstanceID());
		File file_tar = new File(IOUtil.mergeFolderAndFilePath(logDir, filename_tar));
		if (file_tar.exists()) {
			FileUtils.forceDelete(file_tar);
		}
		File file = new File(IOUtil.mergeFolderAndFilePath(logDir, filename));
		if (file.exists()) {
			FileUtils.forceDelete(file);
		}
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQueryHostInsts() throws KeeperException, InterruptedException {

		String jsonStr = "";
		if (!request.getParameterNames().contains("parent")) {
			jsonStr = "[]";
		} else {
			final String parentid = request.getParameter("parent");
			String no = request.getParameter("no");
			if (StringUtil.isNull(parentid)) {
				jsonStr = "[]";
			} else {
				// String path = StringUtil.isNull(parentid) ? duckulaAssit.getPathInstance()
				// : String.format("%s/%s", duckulaAssit.getPathInstance(), parentid);
				DbInstance temp = JSONObject
						.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(parentid)), DbInstance.class);

				IConvertValue<Object> conver = new IConvertValue<Object>() {
					@Override
					public String getStr(Object keyObj) {
						NodeInfo temp = (NodeInfo) keyObj;
						return String.format("%s:%s", temp.getNodeId(), temp.getNodeType());
					}
				};
				int noint = Integer.parseInt(no);
				jsonStr = JSONUtil.getJsonForList(
						(noint == 1 ? temp.getNodesFirst()
								: (noint == 2 ? temp.getNodesSecond() : temp.getNodesThird())),
						new IConvertValue[] { null, conver }, "nodeId,value", ",text");
			}
		}
		return TapestryAssist.getTextStreamResponse(jsonStr);
	}

	public TextStreamResponse onQueryTasks() throws KeeperException, InterruptedException {
		final String queryDbId = request.getParameter("queryDbId");
		final String namespace = request.getParameter("namespace");
		List<String> taskNodes = ZkClient.getInst().getChildren(ZkPath.tasks.getRoot());
		List<Task> tasks = CollectionFactory.newList();
		for (String nodeName : taskNodes) {
			Task temp = ZkUtil.buidlTask(nodeName);
			if (temp != null) {
				tasks.add(temp);
			}
		}

		CollectionUtils.filter(tasks, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Task temp = (Task) object;
				if (StringUtil.isNotNull(queryDbId) && !queryDbId.equals(temp.getDbinst())) {
					return false;
				}
				if (!TaskPattern.isNeedServer() && StringUtil.isNotNull(namespace)&&!"all".equalsIgnoreCase(namespace)
						&& !namespace.equals(temp.getNamespace())) {
					return false;
				}
				return true;
			}
		});

		String retstr = EasyUiAssist.getJsonForGrid(tasks, new String[] { "id", "ip", "rules", "senderEnum" },
				new IConvertValue[] { null, null, null, null, null }, tasks.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSaveOfflineTask() throws KeeperException, InterruptedException {
		final TaskOffline taskparam = TapestryAssist.getBeanFromPage(TaskOffline.class, requestGlobals);
		if (taskparam.getTimeBegin() != null) {
			taskparam.setLimitType(1);
		} else if (StringUtil.isNull(taskparam.getGtidBegin()) && StringUtil.isNull(taskparam.getGtidEnd())) {
			taskparam.setLimitType(2);
		} else {
			return req.retErrorInfo("不支持的限定类型，现在只支持时间和gtid两种类型");
		}
		Task buidlTask = ZkUtil.buidlTask(taskparam.getTaskOnlineId());
		if (buidlTask == null) {
			return req.retErrorInfo("需要在线task");
		}
		taskparam.setTaskOnline(buidlTask);
		taskparam.setBinlogFiles(taskparam.getBinlogFiles().replaceAll(".tar", ""));
		// Stat stat = ZkUtil.exists(ZkPath.tasksoffline, taskparam.getId());
		ZkClient.getInst().createOrUpdateNode(ZkPath.tasksofflines.getPath(taskparam.getId()),
				JSONObject.toJSONString(taskparam));
		return req.retSuccInfo("保存Task成功");
	}

}
