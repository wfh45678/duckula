package net.wicp.tams.duckula.ops.pages.duckula;

import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import net.wicp.tams.common.apiext.NumberUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

public class ViewServer {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IDuckulaAssit duckulaAssit;
	@Inject
	private IReq req;

	// String queryServerUrlformat = "http://%s:%s/duckula-server/monitorshow";

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() throws KeeperException, InterruptedException {
		final Server serverParam = TapestryAssist.getBeanFromPage(Server.class, requestGlobals);
		List<Server> servers = duckulaAssit.findAllServers();
		List<Server> retlist = (List<Server>) CollectionUtils.select(servers, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Server temp = (Server) object;
				boolean ret = true;
				if ("localhost".equalsIgnoreCase(temp.getIp())) {
					return false;
				}
				if (StringUtil.isNotNull(serverParam.getIp())) {
					ret = temp.getIp().indexOf(serverParam.getIp()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}
		});

		Server.packageResources(retlist);

		IConvertValue<String> freeConv = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				return keyObj + "M";
			}
		};

		IConvertValue<String> cpuConv = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				if (StringUtil.isNull(keyObj)) {
					return "未统计";
				} else {
					double d = Double.parseDouble(keyObj);
					BigDecimal temp = NumberUtil.handleScale(d, 2);
					return temp.doubleValue() + "%";
				}

			}
		};

		String retstr = EasyUiAssist.getJsonForGrid(retlist,
				new String[] { "ip", "name", "serverPort", "remark", "mi.freeMemory,fm", "mi.totalMemory,am" },
				new IConvertValue[] { null, null, null, null, freeConv, freeConv }, retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

}
