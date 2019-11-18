package net.wicp.tams.duckula.ops.pages.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.EsAssit;
import net.wicp.tams.common.es.bean.AliasesBean;
import net.wicp.tams.common.es.bean.IndexBean;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.common.web.J2EEAssist;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;
import net.wicp.tams.duckula.plugin.pluginAssit;

@Slf4j
public class IndexManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	/*
	 * @SessionState(create = false) private ESClient eSClient;
	 * 
	 * @SessionState(create = false) private Session session; private boolean
	 * eSClientExists; private boolean sessionExists;
	 */

	private static Map<String, ESClient> esclientmap = new HashMap<>();

	public static ESClient getESClient(String cluster) {
		Validate.isTrue(StringUtil.isNotNull(cluster));
		if (!esclientmap.containsKey(cluster)) {
			synchronized (IndexManager.class) {
				if (!esclientmap.containsKey(cluster)) {
					Properties configMiddleware =ConfUtil.configMiddleware(MiddlewareType.es, cluster);
					ESClient eSClient = new ESClient(configMiddleware);
					esclientmap.put(cluster, eSClient);
				}
			}
		}
		return esclientmap.get(cluster);
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery(String cluster) {
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		if (StringUtil.isNull(cluster)) {
			return TapestryAssist.getTextStreamResponse(EasyUiAssist.getJsonForGridEmpty());
		}

		// ES的索引
		List<IndexBean> queryIndexs = getESClient(cluster)
				.queryIndex(StringUtil.isNotNull(mappingparam.getIndex()) ? mappingparam.getIndex() : null);

		List<AliasesBean> queryAliases = getESClient(cluster)
				.queryAliases(StringUtil.isNotNull(mappingparam.getIndex()) ? mappingparam.getIndex() : null);
		String alias = request.getParameter("alias");
		Set<String> indexSet = new TreeSet<>();
		if (StringUtil.isNotNull(alias)) {
			for (AliasesBean aliase : queryAliases) {
				if (aliase.getAlias().startsWith(alias)) {
					indexSet.add(aliase.getIndex());
				}
			}
		}

		List<Mapping> mappings = ZkUtil.findAllIndex();
		List<Mapping> retlist = (List<Mapping>) CollectionUtils.select(mappings, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Mapping temp = (Mapping) object;
				boolean ret = true;
				if (StringUtil.isNotNull(mappingparam.getIndex())) {
					ret = temp.getId().indexOf(mappingparam.getIndex()) >= 0;
					if (!ret) {
						return false;
					}
				}
				if (StringUtil.isNotNull(mappingparam.getType())) {
					ret = temp.getType().indexOf(mappingparam.getType()) >= 0;
					if (!ret) {
						return false;
					}
				}

				if (StringUtil.isNotNull(alias)) {
					boolean retvalue = indexSet.contains(temp.getIndex());
					return retvalue;
				}
				return ret;
			}
		});

		IConvertValue<String> isExitMap = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				String isExit = "不存在";
				for (IndexBean indexBean : queryIndexs) {
					if (indexBean.getIndex().equals(keyObj)) {
						isExit = "存在";
						break;
					}
				}
				return isExit;
			}
		};

		IConvertValue<String> aliasesMap = new IConvertValue<String>() {
			@Override
			public String getStr(String keyObj) {
				List<String> aliaseList = new ArrayList<>();
				for (AliasesBean queryAliase : queryAliases) {
					if (queryAliase.getIndex().equals(keyObj)) {
						aliaseList.add(queryAliase.getAlias());
					}
				}
				return CollectionUtil.listJoin(aliaseList, ",");
			}
		};

		String retstr = EasyUiAssist.getJsonForGridAlias2(retlist, new String[] { "index,isExit", "index,alias" },
				CollectionUtil.newMap("isExit", isExitMap, "alias", aliasesMap), retlist.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	// 用于直接通过连接地址得到索引（无zk信息的索引）
	public TextStreamResponse onQueryIndex(String cluster) {
		List<IndexBean> queryIndexs = getESClient(cluster).queryIndex(null);
		String retstr = EasyUiAssist.getJsonForGridAlias(queryIndexs, queryIndexs.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	// 用于级联得到索引（包有zk信息的索引）
	public TextStreamResponse onQueryIndex() {
		if (!request.getParameterNames().contains("parent") || StringUtil.isNull(request.getParameter("parent"))) {
			String retlist = JSONUtil.getJsonForListSimple(null);
			return TapestryAssist.getTextStreamResponse(retlist);
		} else {
			final String parentid = request.getParameter("parent");
			TextStreamResponse onQuery = onQuery(parentid);
			JSONObject parseObject;
			try {
				parseObject = JSONObject.parseObject(IOUtil.slurp(onQuery.getStream()));
			} catch (IOException e) {
				throw new RuntimeException("");
			}
			if (parseObject.containsKey("rows")) {
				JSONArray jsonArray = parseObject.getJSONArray("rows");
				return TapestryAssist.getTextStreamResponse(jsonArray.toJSONString());
			} else {
				String retlist = JSONUtil.getJsonForListSimple(null);
				return TapestryAssist.getTextStreamResponse(retlist);
			}
		}
	}

	public TextStreamResponse onQueryMiddlewareType(String middlewareTypeStr) {
		if(StringUtil.isNull(middlewareTypeStr)) {
			return TapestryAssist.getTextStreamResponse("[]"); 
		}
		MiddlewareType middlewareType = MiddlewareType.valueOf(middlewareTypeStr);
		String eleJson = middlewareType.getEleJson();
		return TapestryAssist.getTextStreamResponse(eleJson);
	}

	public TextStreamResponse onQueryMiddlewareType() {
		if (!request.getParameterNames().contains("parent")) {
			String retlist = JSONUtil.getJsonForListSimple(null);
			return TapestryAssist.getTextStreamResponse(retlist);
		} else {
			final String parentid = request.getParameter("parent");
			return onQueryMiddlewareType(parentid);
		}
	}

	public TextStreamResponse onChangeIndexAlias(String cluster) {
		String oldIndex = request.getParameter("oldIndex");
		String newIndex = request.getParameter("newIndex");
		String[] aliass = request.getParameter("alias").split(",");
		List<IndexBean> oldIndexs = getESClient(cluster).queryIndex(oldIndex);
		Result retResult = null;
		if (oldIndexs.size() == 0) {
			retResult = getESClient(cluster).aliasCreate(newIndex, aliass);
		} else {
			retResult = getESClient(cluster).indexReplace(newIndex, oldIndex, false, aliass);
		}
		return TapestryAssist.getTextStreamResponse(retResult);
	}

	public TextStreamResponse onCreateIndexAlias(String cluster) {
		String oldIndex = request.getParameter("oldIndex");
		String[] aliass = request.getParameter("alias").split(",");
		Result retResult = getESClient(cluster).aliasCreate(oldIndex, aliass);
		return TapestryAssist.getTextStreamResponse(retResult);
	}

	public TextStreamResponse onCreateIndex() {
		//String requestPayload = J2EEAssist.getRequestPayload(requestGlobals.getHTTPServletRequest());///////////////////////////////////////////////zjh
		//System.out.println(requestPayload);
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		// request.

		DbInstance temp = ZkClient.getInst().getDateObj(
				String.format("%s/%s", ZkPath.dbinsts.getRoot(), mappingparam.getDbinst()), DbInstance.class);
		java.sql.Connection conn = JdbcConnection.getConnectionMyql(temp.getUrl(), temp.getPort(), temp.getUser(),
				temp.getPwd(), temp.getIsSsh());
		String[][] cols = MySqlAssit.getCols(conn, mappingparam.getDb(), mappingparam.getTb(), YesOrNo.yes);// TODO

		if (StringUtil.isNotNull(mappingparam.getDb1()) && StringUtil.isNotNull(mappingparam.getTb1())) {
			String[][] cols1 = MySqlAssit.getCols(conn, mappingparam.getDb1(), mappingparam.getTb1(), YesOrNo.yes);
			String[] nameAry = CollectionUtil.arrayMerge(String[].class, cols[0], cols1[0]);
			String[] typeAry = CollectionUtil.arrayMerge(String[].class, cols[1], cols1[1]);
			cols = new String[][] { nameAry, typeAry };
		}
		try {
			conn.close();
		} catch (Exception e) {
		}
		String contentjson = "{}";
		if (ArrayUtils.isNotEmpty(cols) && !"_rowkey_".equals(cols[0][0])) {// 有主键
			contentjson = EsAssit.packIndexContent(cols[0], cols[1], mappingparam.buildRelaNodes());
		}
		return TapestryAssist.getTextStreamResponse(contentjson);
	}

	public TextStreamResponse onSave(String cluster) {
		String requestPayload = J2EEAssist.getRequestPayload(requestGlobals.getHTTPServletRequest());///////////////////////////////////////////////zjh
		System.out.println(requestPayload);
		
		JSONObject json = JSONUtil.getJsonFromUrlStr(requestPayload);
		final Mapping mappingparam = JSONUtil.getBeanFromJson(Mapping.class,  json);
		
		//final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		if (StringUtil.isNull(mappingparam.getContent())) {
			DbInstance temp = ZkClient.getInst().getDateObj(
					String.format("%s/%s", ZkPath.dbinsts.getRoot(), mappingparam.getDbinst()), DbInstance.class);
			java.sql.Connection conn = JdbcConnection.getConnectionMyql(temp.getUrl(), temp.getPort(), temp.getUser(),
					temp.getPwd(), temp.getIsSsh());
			String[][] cols = MySqlAssit.getCols(conn, mappingparam.getDb(), mappingparam.getTb(), YesOrNo.yes);// TODO
			if (StringUtil.isNotNull(mappingparam.getDb1()) && StringUtil.isNotNull(mappingparam.getTb1())) {
				String[][] cols1 = MySqlAssit.getCols(conn, mappingparam.getDb1(), mappingparam.getTb1(), YesOrNo.yes);
				String[] nameAry = CollectionUtil.arrayMerge(String[].class, cols[0], cols1[0]);
				String[] typeAry = CollectionUtil.arrayMerge(String[].class, cols[1], cols1[1]);
				cols = new String[][] { nameAry, typeAry };
			}
			try {
				conn.close();
			} catch (Exception e) {
			}
			String contentjson = EsAssit.packIndexContent(cols[0], cols[1], mappingparam.buildRelaNodes());
			mappingparam.setContent(contentjson);
		}

		int indexOf = mappingparam.getContent().indexOf("\"");
		if (indexOf >= 0) {
			return TapestryAssist.getTextStreamResponse(Result.getError("json内容请使用单引号"));
		}
		MappingBean proMappingBean = null;
		try {
			proMappingBean = MappingBean.proMappingBean(mappingparam.getContent());
		} catch (Exception e) {
			return TapestryAssist.getTextStreamResponse(Result.getError(e.getMessage()));
		}

		if (StringUtil.isNotNull(mappingparam.getId())) {// 修改
			getESClient(cluster).indexDel(mappingparam.getIndex());
		} else {
			mappingparam.setId(mappingparam.getIndex() + "-" + mappingparam.getType());
		}
		Result createIndex = getESClient(cluster).indexCreate(mappingparam.getIndex(), mappingparam.getType(),
				mappingparam.getShardsNum(), mappingparam.getReplicas(), null, proMappingBean);
		if (!createIndex.isSuc()) {
			return TapestryAssist.getTextStreamResponse(createIndex);
		}
		Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(ZkPath.mappings.getPath(mappingparam.getId()),
				JSONObject.toJSONString(mappingparam));
		return TapestryAssist.getTextStreamResponse(createOrUpdateNode);
	}

	public TextStreamResponse onDel(String cluster) {
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		Result indexDel = getESClient(cluster).indexDel(mappingparam.getIndex());
		if(!indexDel.isSuc()&& indexDel.getMessage().contains("index_not_found_exception")) {//如果没有找到可以放过
			indexDel=Result.getSuc();
		}		
		if (indexDel.isSuc()) {
			ZkUtil.del(ZkPath.mappings, mappingparam.getId());
		}
		return TapestryAssist.getTextStreamResponse(indexDel);
	}

	@Property
	private String cluster;

	void onActivate(String cluster) {
		this.cluster = cluster;
	}
}
