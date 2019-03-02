package net.wicp.tams.duckula.ops.pages.es;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.tapestry5.SymbolConstants;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.annotations.Symbol;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.upload.services.MultipartDecoder;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.os.bean.FileBean;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.BusiPlugin;
import net.wicp.tams.duckula.common.constant.PluginType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

@Slf4j
public class BusiPluginManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	@Inject
	private MultipartDecoder decoder;

	@Property
	@Inject
	@Symbol(SymbolConstants.CONTEXT_PATH)
	private String contextPath;

	public boolean isNeedServer() {
		return TaskPattern.isNeedServer();
	}

	public String getColDifferent() {
		if (isNeedServer()) {
			return "{field:'fileExitServer',width:100,title:'同步状态',formatter:showstatus},";
		} else {
			return "";
		}
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() {
		String pluginpath = PluginType.consumer.getPluginDir(false);
		File pluginDir = new File(pluginpath);
		final List<String> fileNames = new ArrayList<>();
		if (!pluginDir.exists()) {
			try {
				FileUtils.forceMkdir(pluginDir);
			} catch (Exception e) {
				log.error("创建用户插件目录失败", e);
			}
		} else {
			File[] files = pluginDir.listFiles();
			for (File file : files) {
				if (!file.isDirectory()) {
					fileNames.add(file.getName());
				}
			}
		}

		IConvertValue<String> fileExit = new IConvertValue<String>() {
			@Override
			public String getStr(String pluginFileName) {
				if (StringUtil.isNull(pluginFileName)) {
					return "否";
				}
				if (fileNames.contains(pluginFileName)) {
					return "是";
				} else {
					return "否";
				}
			}
		};

		final BusiPlugin busiPluginQuery = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		List<BusiPlugin> plugins = ZkUtil.findAllObjs(ZkPath.busiplugins, BusiPlugin.class);
		List<BusiPlugin> retlist = (List<BusiPlugin>) CollectionUtils.select(plugins, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				BusiPlugin temp = (BusiPlugin) object;
				boolean ret = true;
				if (StringUtil.isNotNull(busiPluginQuery.getProjectName())) {
					ret = temp.getProjectName().indexOf(busiPluginQuery.getProjectName()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}
		});

		String retstr = null;
		if (isNeedServer()) {
			Map<String, List<FileBean>> serverplugs = new HashMap<>();
			List<Server> servers = ZkUtil.findAllObjs(ZkPath.servers, Server.class);

			for (Server server : servers) {
				if ("localhost".equals(server.getIp())) {
					continue;
				}
				if (server.getIsInit() == YesOrNo.yes) {
					SSHConnection conn = DuckulaUtils.getConn(server);
					List<FileBean> llFiles = conn.llFile(PluginType.consumer.getPluginDir(true), YesOrNo.no);
					DuckulaUtils.returnConn(server, conn);
					serverplugs.put(server.getIp(), llFiles);
				}
			}

			IConvertValue<Object> fileExitServer = new IConvertValue<Object>() {
				@Override
				public String getStr(Object keyObj) {// 得到Id
					BusiPlugin opsPlug = (BusiPlugin) keyObj;
					// String ret = "0";// -1:有异常，需要处理 0：不存在 1:存在但需要更新 2:存在无需更新
					for (Server server : servers) {
						if ("localhost".equals(server.getIp())) {
							continue;
						}
						List<FileBean> list = serverplugs.get(server.getIp());
						if (list == null) {
							return "-1";// 服务器上插件不存在
						}
						boolean isExit = false;
						for (FileBean busiPlugin : list) {
							if (busiPlugin.getFileName().equals(opsPlug.getPluginFileName().replace(".tar", ""))) {
								try {
									Date opsDate = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc()
											.parse(opsPlug.getLastUpdateTime());
									if (opsDate.after(busiPlugin.getLastUpdateTime())) {
										return "1";// 插件较旧，需要更新
									}
									isExit = true;
									break;
								} catch (Exception e) {
									log.error("插件更新时间比较失败", e);
									return "-1";// 对比插件时异常
								}
							}
						}
						if (!isExit) {
							return "0";// 没有要找的插件
						}
					}
					return "2";// 插件正常
				}
			};

			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "projectName", "update", "lastUpdateTime", "pluginFileName",
							"pluginFileName,fileExit", ",fileExitServer" },
					new IConvertValue[] { null, null, null, null, null, fileExit, fileExitServer }, retlist.size());
		} else {

			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "projectName", "update", "lastUpdateTime", "pluginFileName",
							"pluginFileName,fileExit" },
					new IConvertValue[] { null, null, null, null, null, fileExit }, retlist.size());
		}
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSave() {
		final BusiPlugin busiPlugin = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		busiPlugin.setLastUpdateTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(
				ZkPath.busiplugins.getPath(busiPlugin.getId()), JSONObject.toJSONString(busiPlugin));
		return TapestryAssist.getTextStreamResponse(createOrUpdateNode);
	}

	public void onSaveFile() {
		String pluginpath = PluginType.consumer.getPluginDir(false);
		File pluginDir = new File(pluginpath);
		List<File> uploadlist = req.saveUpload(pluginDir);// 新文件会覆盖旧文件
		String id = request.getParameter("id");
		BusiPlugin busiPlugin = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.busiplugins.getPath(id)),
				BusiPlugin.class);
		File pluginFile = uploadlist.get(0);
		busiPlugin.setPluginFileName(pluginFile.getName());
		busiPlugin.setLastUpdateTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		ZkClient.getInst().createOrUpdateNode(ZkPath.busiplugins.getPath(busiPlugin.getId()),
				JSONObject.toJSONString(busiPlugin));
	}

	public TextStreamResponse onUploadPlug() throws ClientProtocolException, IOException {
		final BusiPlugin busiPluginSyc = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		List<Server> servers = ZkUtil.findAllObjs(ZkPath.servers, Server.class);
		String plugFilePath = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDir(false),
				busiPluginSyc.getPluginFileName());
		try {
			for (Server server : servers) {
				if ("localhost".equals(server.getIp())) {
					continue;
				}
				SSHConnection conn = DuckulaUtils.getConn(server);
				conn.scpToDir(plugFilePath, PluginType.consumer.getPluginDir(true));
				String remoteFile = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDir(true),
						busiPluginSyc.getPluginFileName());
				conn.tarX(remoteFile);
				DuckulaUtils.returnConn(server, conn);
			}
		} catch (Exception e) {
			return TapestryAssist.getTextStreamResponse(Result.getError(e.getMessage()));
		}
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public TextStreamResponse onDel() {
		final BusiPlugin busiPlugin = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		Result del = ZkUtil.del(ZkPath.busiplugins, busiPlugin.getId());
		return TapestryAssist.getTextStreamResponse(del);
	}
}
