package net.wicp.tams.duckula.kafka.consumer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONObject;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.es.Action;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.common.es.EsData.Builder;
import net.wicp.tams.common.es.EsObj;
import net.wicp.tams.common.es.RelaValue;
import net.wicp.tams.common.es.UpdateSet;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.bean.MappingBean.Propertie;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.common.es.client.threadlocal.EsClientThreadlocal;
import net.wicp.tams.duckula.client.DuckulaAssit;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

public class ConsumerEsImpl extends ConsumerAbs<EsData.Builder> {

	public ConsumerEsImpl(Consumer consumer) {
		super(consumer);
	}

	// 序列化后用的ruleMap,只存index和type，如果需要发不同的ES，要确保发送的index和type不能相同，否则后面的规则会覆盖前面
	private Map<String, Rule> ruleMapToEs = new HashMap<>();
	// 存储rela的json对象
	private Map<String, JSONObject> relaMapToEs = new HashMap<>();

	private final String keyFormate = "%s:%s";

	@Override
	public Builder packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule) {
		String index = rule.getItems().get(RuleItem.index);
		String type = StringUtil.isNull(rule.getItems().get(RuleItem.type)) ? "_doc"
				: rule.getItems().get(RuleItem.type);
		String key = String.format(keyFormate, index, type);
		if (!ruleMapToEs.containsKey(key)) {
			ruleMapToEs.put(key, rule);
		}
		// 查看index是否有关联关系，一般有2张表肯定有关联关系
		if (!relaMapToEs.containsKey(key)) {
			ESClient esClient = EsClientThreadlocal.createPerThreadEsClient();
			Map<String, Propertie> queryMapping_tc_all = esClient.queryMapping_tc_all(index, type);
			if (queryMapping_tc_all.containsKey(Conf.get("common.es.assit.rela.key"))) {
				JSONObject relations = queryMapping_tc_all.get(Conf.get("common.es.assit.rela.key")).getRelations();
				relaMapToEs.put(key, relations);
			} else {
				relaMapToEs.put(key, null);
			}
		}

		JSONObject relaJson = relaMapToEs.get(key);
		Builder esDataBuilder = EsData.newBuilder();
		esDataBuilder.setIndex(index);
		esDataBuilder.setType(type);
		esDataBuilder.setUpdateSet(UpdateSet.newBuilder().setUpsert(true).build());
		esDataBuilder.setAction(duckulaEvent.getOptType() == OptType.delete ? Action.delete : Action.update);
		EsObj.Builder esObjBuilder = EsObj.newBuilder();
		esObjBuilder.putAllSource(datamap);
		boolean isroot = MappingBean.isRoot(relaJson, duckulaEvent.getTb());
		if (isroot) {// 根元素或是没有关联关联的索引
			String keyColName = rule.getItems().get(RuleItem.key);
			String idStr = DuckulaAssit.getValueStr(duckulaEvent, keyColName);
			esObjBuilder.setId(idStr);
			if (relaJson != null) {
				esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(duckulaEvent.getTb()));// tams_relations
			}
			esDataBuilder.addDatas(esObjBuilder);
		} else {// 有关联关系且不是根元素
			String keyColName = rule.getItems().get(RuleItem.relakey);
			String keyName = "";
			if (StringUtil.isNotNull(keyColName)) {
				String[] splitAry = keyColName.split("\\|");
				if (splitAry.length == 1) {
					keyName = splitAry[0];
				} else {
					for (String ele : splitAry) {
						String[] tempAry = ele.split(":");
						if (duckulaEvent.getTb().equalsIgnoreCase(tempAry[0])) {
							keyName = tempAry[1];
							break;
						}
					}
				}
			}
			if (StringUtil.isNull(keyName)) {// 没有配置就取第1个字段
				keyName = duckulaEvent.getCols(0);
			}

			String relaName = MappingBean.getRelaName(relaJson, duckulaEvent.getTb());
			String[] relaNameAry = relaName.split(":");
			String parentId = DuckulaAssit.getValueStr(duckulaEvent, relaNameAry[1]);
			// 找id
			String idstr = DuckulaAssit.getValueStr(duckulaEvent, duckulaEvent.getCols(0));// TODO 子表暂时使用第一个字段， 后续需要改为配置
			// String idstr = DuckulaAssit.getValueStr(duckulaEvent, keyName);
			esObjBuilder.setId(String.format("%s:%s", duckulaEvent.getTb(), idstr));// 有可能与主表id相同把主表的ID冲掉
			if (StringUtils.isBlank(parentId)) {// 关联关系没有parent
				errorlog.error(esObjBuilder.toString());// 打错误日志跳过
			} else {
				esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(relaName).setParent(parentId));// tams_relations
				esDataBuilder.addDatas(esObjBuilder);
			}
		}
		return esDataBuilder;
	}

	@Override
	public Result doSend(List<Builder> datas) {
		if (CollectionUtils.isEmpty(datas)) {
			return Result.getSuc();
		}
		List<EsData> datasSend = new ArrayList<EsData>();
		for (Builder builder : datas) {
			datasSend.add(builder.build());
		}
		Result sendResult = EsClientThreadlocal.createPerThreadEsClient().docWriteBatch_tc(datasSend);
		return sendResult;
	}

	@Override
	public boolean checkDataNull(Builder data) {
		return data.getDatasCount() == 0;
	}

}
