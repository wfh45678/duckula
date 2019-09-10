package net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.WorkHandler;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.Action;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.common.es.EsData.Builder;
import net.wicp.tams.common.es.EsObj;
import net.wicp.tams.common.es.RelaValue;
import net.wicp.tams.common.es.UpdateSet;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.bean.MappingBean.Propertie;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.dump.elasticsearch.bean.EventDump;

/****
 * JDK1.5规范说明：
 * 1.垃圾回收机制可以自动关闭Statement和Connection；
 * 2.Statement关闭会导致ResultSet关闭；
 * 3.Connection关闭不一定会导致Statement关闭。
 * 
 * @author 偏锋书生
 *
 *         2018年4月26日
 */
@Slf4j
public class BaseDataHander implements WorkHandler<EventDump> {
	private Connection connection;
	private PreparedStatement stmt;
	private final Mapping mapping;
	private final String temp;
	private final String[] primarys;
	private static JSONObject relaObj;// relaObj.isEmpty 是已初始化但没有关联关系的索引
	private final String[] dbtb;
	private static String[] needColNames;

	private static final org.slf4j.Logger errorlog = org.slf4j.LoggerFactory.getLogger("errorBinlog");

	public BaseDataHander(Dump dump, Mapping mapping) {
		this.mapping = mapping;
		this.primarys = dump.getPrimarys();
		// tb值
		this.dbtb = dump.getDb_tb().split("\\.");
		// String arrayJoin = CollectionUtil.arrayJoin(mapping.findColNames(), "`,`");
		needColNames = filterColName();
		String arrayJoin = CollectionUtil.arrayJoin(needColNames, "`,`");
		this.temp = String.format("select `%s` %s and %s >=? and %s<=?", arrayJoin, dump.packFromstr(), primarys[0],
				primarys[0]);
		// 初始化关联关系
		if (relaObj == null) {
			synchronized (BaseDataHander.class) {
				if (relaObj == null) {
					Map<String, Propertie> queryMapping_tc_all = ESClientOnlyOne.getInst().getESClient()
							.queryMapping_tc_all(mapping.getIndex(), mapping.getType());
					if (queryMapping_tc_all.containsKey(Conf.get("common.es.assit.rela.key"))) {
						relaObj = queryMapping_tc_all.get(Conf.get("common.es.assit.rela.key")).getRelations();
					} else {
						relaObj = new JSONObject();
					}
					// ESClientOnlyOne.getInst().getESClient().close(); todo
				}
			}
		}

	}

	/**
	 * 过滤数据库不存在的列名 当有关联关系的时候存在这种情况
	 * 
	 * @return
	 */
	private String[] filterColName() {
		String[] indexColNames = mapping.findColNames();
		Connection tempConn = DruidAssit.getConnection();
		String[][] cols = MySqlAssit.getCols(tempConn, dbtb[0], dbtb[1], YesOrNo.yes);
		try {
			tempConn.close();
		} catch (Exception e) {
		}
		String[] retEles = CollectionUtil.arrayAnd(String[].class, indexColNames, cols[0]);
		return retEles;
	}

	@Override
	public void onEvent(EventDump event) throws Exception {
		Thread.currentThread().setName("BaseDataHanderThread");
		initConn();
		JdbcAssit.setPreParam(stmt, event.getBeginId(), event.getEndId());
		ResultSet rs = stmt.executeQuery();
		Builder esDataBuilder = EsData.newBuilder();
		esDataBuilder.setIndex(this.mapping.getIndex());
		esDataBuilder.setType(this.mapping.getType());
		esDataBuilder.setAction(Action.update);
		esDataBuilder.setUpdateSet(UpdateSet.newBuilder().setUpsert(true).build());
		boolean hasprimarys = ArrayUtils.isNotEmpty(primarys);
		// int i = 0;
		while (rs.next()) {
			// long currentTimeMillis = System.currentTimeMillis();
			EsObj.Builder esObjBuilder = EsObj.newBuilder();

			Map<String, String> datamap = new HashMap<>();
			for (String colName : needColNames) {
				String valuestr = rs.getString(colName);
				if (valuestr != null) {
					datamap.put(colName, valuestr);
				}
			}
			esObjBuilder.putAllSource(datamap);
			if (hasprimarys) {
				String[] values = new String[primarys.length];
				for (int j = 0; j < values.length; j++) {
					values[j] = datamap.get(primarys[j]);
				}
				String idstr = CollectionUtil.arrayJoin(values, "-");
				if (StringUtils.isEmpty(idstr)) {
					log.error("id是空值");
					continue;
				}
				// 关联关系支持 20181201
				boolean isroot = MappingBean.isRoot(relaObj, dbtb[1]);
				if (isroot) {// 根元素或是没有关联关联的索引
					esObjBuilder.setId(idstr);
					if (relaObj != null && !relaObj.isEmpty()) {
						esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(dbtb[1]));// tams_relations
					}
				} else {// 有关联关系且不是根元素
					String relaName = MappingBean.getRelaName(relaObj, dbtb[1]);
					String[] relaNameAry = relaName.split(":");
					String parentId = rs.getString(relaNameAry[1]);
					esObjBuilder.setId(String.format("%s:%s", dbtb[1], idstr));// 有可能与主表id相同把主表的ID冲掉
					if (StringUtils.isBlank(parentId)) {// 关联关系没有parent
						errorlog.error(esObjBuilder.toString());// 打错误日志跳过
						continue;
					}
					esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(relaName).setParent(parentId));// tams_relations
				}
			}
			esDataBuilder.addDatas(esObjBuilder.build());
		}
		event.setEsDataBuilder(esDataBuilder);
		try {
			rs.close();
		} catch (Exception e) {
			log.error("关闭rs失败", e);
		}
		// log.info("batch over,start:{},end:{},use time:{}", event.getBeginId(),
		// event.getEndId(),
		// System.currentTimeMillis() - curTime);
	}

	private void initConn() {
		while (true) {
			try {
				if (this.connection == null || connection.isClosed()) {
					// 先关闭旧的stmt;
					if (this.stmt != null && this.stmt.isClosed()) {
						this.stmt.close();
					}
					this.connection = DruidAssit.getConnection();
					this.stmt = this.connection.prepareStatement(this.temp);
					this.stmt.setFetchSize(Publisher.numDuan);
				}
				break;
			} catch (Exception e) {
				log.error("数据库连接不上", e);
				try {
					Thread.sleep(1000);
				} catch (Exception e2) {
				}
				continue;
			}
		}
	}

}
