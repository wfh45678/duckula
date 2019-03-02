package net.wicp.tams.duckula.plugin.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.es.EsAssit;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.common.es.bean.MappingBean.DataTypes;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectExceptionRuntime;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;

@Slf4j
public class ReceiveElasticsearch extends ReceiveAbs {

	public ReceiveElasticsearch(JSONObject paramObjs) {
		super(paramObjs);
		Conf.overProp(props);
	}

	@Override
	public boolean receiveMsg(DuckulaPackage duckulaPackage, Rule rule) {
		String idKey = rule.getItems().get(RuleItem.key) == null ? duckulaPackage.getEventTable().getCols()[0]
				: rule.getItems().get(RuleItem.key);
		String index = rule.getItems().get(RuleItem.index);
		Validate.notBlank(index);
		String type = StringUtil.hasNull(rule.getItems().get(RuleItem.type), "_doc");
		List<EsData> esDatas = new ArrayList<>();
		String[][] datas = (duckulaPackage.getEventTable().getOptType() == OptType.delete) ? duckulaPackage.getBefores()
				: duckulaPackage.getAfters();
		for (String[] data : datas) {
			EsData esDataUpsert = null;
			if (duckulaPackage.getEventTable().getOptType() == OptType.delete) {
				esDataUpsert = EsAssit.esDataDel(data, duckulaPackage.getEventTable().getCols(), idKey, index, type);
			} else {
				esDataUpsert = EsAssit.esDataUpsert(data, duckulaPackage.getEventTable().getCols(), idKey, index, type);
			}
			esDatas.add(esDataUpsert);
		}

		ESClient eSClient = ESClientOnlyOne.getInst().getESClient();
		Result writeResult = eSClient.docWriteBatch_tc(esDatas);
		return writeResult.isSuc();
	}

	@Override
	public boolean receiveMsg(List<SingleRecord> data, Rule rule) {
		throw new ProjectExceptionRuntime(ExceptAll.project_nosupport, "ES接收者不支持序列化，序列化请配置成‘无’");
	}

	public static void main(String[] args) {
		Map<String, DataTypes> queryMapping_tc = ESClientOnlyOne.getInst().getESClient()
				.queryMapping_tc("file_type_test_v1", "_doc");
		log.info("birthday===========" + queryMapping_tc.get("birthday"));
	}

	@Override
	public boolean isSync() {
		return true;
	}

}
