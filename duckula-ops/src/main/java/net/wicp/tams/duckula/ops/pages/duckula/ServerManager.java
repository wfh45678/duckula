package net.wicp.tams.duckula.ops.pages.duckula;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSON;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.apiext.json.easyuibean.EasyUINode;
import net.wicp.tams.common.apiext.json.easyuibean.EasyUINodeConf;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.exception.ProjectException;
import net.wicp.tams.common.os.SSHAssit;
import net.wicp.tams.common.os.constant.CommandCentOs;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

/***
 * 服务器管理
 * 
 * @author zhoujunhui
 *
 */
@Slf4j
public class ServerManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;
	


	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() throws KeeperException, InterruptedException {
		final Server serverParam = TapestryAssist.getBeanFromPage(Server.class, requestGlobals);
		List<Server> servers = duckulaAssit.findAllServers();
		List<Server> retlist = (List<Server>) CollectionUtils.select(servers, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Server temp = (Server) object;
				boolean ret = true;
				if (StringUtil.isNotNull(serverParam.getIp())) {
					ret = temp.getIp().indexOf(serverParam.getIp()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}

		});

		final Map<String, Integer> maps = duckulaAssit.serverRunTaskNum(retlist);
		IConvertValue<String> taskNum = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				return String.valueOf(maps.get(keyObj));
			}

		};
		TaskTreeBean yesRoot = TaskTreeBean.builder().id("task").text("task任务").build();
		TaskTreeBean noRoot = TaskTreeBean.builder().id("consumer").text("consumer任务").build();
		final List<TaskTreeBean> rootTasks = new ArrayList<>();
		rootTasks.add(yesRoot);
		rootTasks.add(noRoot);

		Map<String, Map<ZkPath, List<String>>> serverRunTaskDetail = duckulaAssit.serverRunTaskDetail(retlist);
		final EasyUINodeConf conf = new EasyUINodeConf("id", "text", "parent");
		IConvertValue<String> taskDetail = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				Map<ZkPath, List<String>> map = serverRunTaskDetail.get(keyObj);
				if (CollectionUtils.isEmpty(map.get(ZkPath.tasks))
						&& CollectionUtils.isEmpty(map.get(ZkPath.consumers))) {
					return "";
				}
				List<TaskTreeBean> treenodes = new ArrayList<>();
				treenodes.addAll(rootTasks);

				for (String taskId : map.get(ZkPath.tasks)) {
					treenodes.add(TaskTreeBean.builder().parent("task").id(taskId).text(taskId).build());
				}
				for (String taskId : map.get(ZkPath.consumers)) {
					treenodes.add(TaskTreeBean.builder().parent("consumer").id(taskId).text(taskId).build());
				}

				List<EasyUINode> roots;
				try {
					roots = EasyUiAssist.getTreeRoot(treenodes, conf);
					String treestr = EasyUiAssist.getTreeFromList(roots);
					return treestr.replace("\"", "'").replace("\n", "").replace(" ", "");
				} catch (Exception e) {
					log.error("转为task树错误", e);
					return "[]";
				}
				// return String.format("all:%s，run:%s，sleep:%s",
				// CollectionUtil.listJoin(allTasks, ","),
				// CollectionUtil.listJoin(yesTasks, ","),
				// CollectionUtil.listJoin(noTasks, ","));
			}
		};

		String retstr = EasyUiAssist.getJsonForGrid(retlist,
				new String[] { "ip", "name", "serverPort", "lockIp", "remark", "isInit", "useDocker","syncConfDate", "ip,taskNum",
						"ip,tasktail" },
				new IConvertValue[] { null, null, null, null, null, null, null,null, taskNum, taskDetail }, retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSave() {
		final Server serverParam = TapestryAssist.getBeanFromPage(Server.class, requestGlobals);
		Result result = ZkClient.getInst().createOrUpdateNode(ZkPath.servers.getPath(serverParam.getIp()),
				JSON.toJSONString(serverParam));
		return TapestryAssist.getTextStreamResponse(result);
	}
	
	/**
	 * 同步配置信息
	 * @return
	 */
	public TextStreamResponse onSyncConf() {
		List<Server> servers = duckulaAssit.findAllServers();
		String curdatestr = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date());
		for (Server server : servers) {
			// 1、登陆
			SSHConnection conn = null;
			try {
				conn = SSHAssit.getConn(server.getIp(), server.getServerPort(), Conf.get("common.os.ssh.username"), Conf.get("common.os.ssh.pwd"),0);
			} catch (ProjectException e) {
				log.error("连接服务器失败", e);
				return TapestryAssist.getTextStreamResponse(Result.getError("连接服务器失败：" + e.getMessage()));
			}
			conn.scpDir(ConfUtil.getDatadir(false), IOUtil.mergeFolderAndFilePath(ConfUtil.getDatadir(true), "/conf"), "0744",new String[] {"conf"});// 
			server.setSyncConfDate(curdatestr);
			ZkClient.getInst().createOrUpdateNode(ZkPath.servers.getPath(server.getIp()),
					JSON.toJSONString(server));
			conn.close();
		}
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public TextStreamResponse onInitServer() {
		final Server serverParam = TapestryAssist.getBeanFromPage(Server.class, requestGlobals);
		String pwd = request.getParameter("pwd");
		if (StringUtil.isNull(pwd)) {
			return TapestryAssist.getTextStreamResponse(Result.getError("需要提供root密码"));
		}
		if ("localhost".equals(serverParam.getIp())) {
			return TapestryAssist.getTextStreamResponse(Result.getError("localhost不需要初始化"));
		}
		// 1、登陆
		SSHConnection conn = null;
		try {
			conn = SSHAssit.getConn(serverParam.getIp(), serverParam.getServerPort(), "root", pwd,0);
		} catch (ProjectException e) {
			log.error("连接服务器失败", e);
			return TapestryAssist.getTextStreamResponse(Result.getError("连接服务器失败：" + e.getMessage()));
		}
		
		// 3、检查是否支持docker
		YesOrNo checkDocker = conn.checkDocker();
		if (checkDocker != null && checkDocker == YesOrNo.yes) {
			serverParam.setUseDocker(YesOrNo.yes);
		} else {
			serverParam.setUseDocker(YesOrNo.no);
		}
		//4、复制配置信息等
		String dataHome=ConfUtil.getDatadir(true);//服务器上的配置目录
		String dataHomeOps=ConfUtil.getDatadir(false);//ops的data配置
		conn.scpDir(dataHomeOps, dataHome, "0744",new String[] {"conf","busi","consumers","sender","serializer"});
		
		//5、初始化logs目录
		File[] logDirs = new File(IOUtil.mergeFolderAndFilePath(dataHomeOps, "logs")).listFiles();
		for (File logDir : logDirs) {
			if(logDir.isDirectory()) {
				Result creatDir = conn.executeCommand(CommandCentOs.mkdir,null,IOUtil.mergeFolderAndFilePath(dataHome, "logs",logDir.getName()));
				if(!creatDir.isSuc()) {
					throw new RuntimeException("创建目录失败:"+creatDir.getMessage());
				}
			}
		}		
		//6、复制程序
		String duckulaPath = requestGlobals.getHTTPServletRequest().getSession().getServletContext().getRealPath("/resource/duckula.tar");
		conn.scp(  duckulaPath, "duckula.tar", "~", "0744");//TODO
		conn.executeCommand(CommandCentOs.tar, null, String.format("%s/duckula.tar -C /opt", "~"));//解压
		
	/*未测试	// 2、上传init.sh文件
				String duckulaHome = System.getenv("DUCKULA_HOME");
				Result scpToDir = conn.scpToDir(String.format("%s/bin/duckula-init.sh", duckulaHome), "/root");
				if (!scpToDir.isSuc()) {
					return TapestryAssist.getTextStreamResponse(scpToDir);
				}*/
		
		// 7、执行shell脚本
		String hosts = MiddlewareType.getHosts();
		Result executeCommand = null;
		String initFilePath = IOUtil.mergeFolderAndFilePath(Conf.get("duckula.ops.homedir"), "/bin/duckula-init.sh");
		if (StringUtil.isNotNull(hosts)) {
			executeCommand = conn.executeCommand(String.format("sh %s %s %s \"%s\"",initFilePath,
					Conf.get("common.os.ssh.username"), Conf.get("common.os.ssh.pwd"), hosts));
		} else {
			executeCommand = conn.executeCommand(String.format("sh %s %s %s",initFilePath,
					Conf.get("common.os.ssh.username"), Conf.get("common.os.ssh.pwd")));
		}

		if (!executeCommand.isSuc()) {
			return TapestryAssist.getTextStreamResponse(executeCommand);
		}

		conn.close();
		serverParam.setIsInit(YesOrNo.yes);
		Result result = ZkClient.getInst().createOrUpdateNode(ZkPath.servers.getPath(serverParam.getIp()),
				JSON.toJSONString(serverParam));
		if (!result.isSuc()) {
			return TapestryAssist.getTextStreamResponse(result);
		}
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public TextStreamResponse onDel() throws KeeperException, InterruptedException {
		final Server serverparam = TapestryAssist.getBeanFromPage(Server.class, requestGlobals);
		Result result = ZkClient.getInst().deleteNode(ZkPath.servers.getPath(serverparam.getIp()));
		return TapestryAssist.getTextStreamResponse(result);
	}




	@Builder
	@Data
	public static class TaskTreeBean {
		private String id;
		private String text;
		private String parent;
	}
}
