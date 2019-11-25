package net.wicp.tams.duckula.ops.pages.duckula;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;
import org.apache.zookeeper.KeeperException;

import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.CountShow;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

public class ViewNum {

	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IDuckulaAssit duckulaAssit;

	@SessionState
	private String namespace;

	private boolean namespaceExists;

	@Inject
	private IReq req;

	public TextStreamResponse onQuery() throws KeeperException, InterruptedException {
		if (!namespaceExists) {
			String jsonStr = EasyUiAssist.getJsonForGridEmpty();
			return TapestryAssist.getTextStreamResponse(jsonStr);
		}

		final String taskId = request.getParameter("id");
		List<CountShow> counts = new ArrayList<>();
		List<String> taskIds = ZkUtil.findSubNodes(ZkPath.counts);
		List<String> fitTasks = DuckulaUtils.findTaskIdByNamespace(namespace);
		CollectionUtils.filter(taskIds, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				String taskIdstr = String.valueOf(object);
				return fitTasks.contains(taskIdstr);
			}
		});
		for (String ele : taskIds) {
			CountShow countShow = ZkClient.getInst().getDateObj(ZkPath.counts.getPath(ele), CountShow.class);
			countShow.setId(ele);
			counts.add(countShow);
		}

		if (StringUtil.isNotNull(taskId)) {
			CollectionUtils.filter(counts, new Predicate() {
				@Override
				public boolean evaluate(Object object) {
					CountShow temp = (CountShow) object;
					return temp.getId().equals(taskId);
				}
			});
		}

		String retstr = EasyUiAssist.getJsonForGridAlias(counts, counts.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onInitCount() throws KeeperException, InterruptedException {

		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public void onActivate(String namespace) {
		this.namespace = namespace;
	}
}
