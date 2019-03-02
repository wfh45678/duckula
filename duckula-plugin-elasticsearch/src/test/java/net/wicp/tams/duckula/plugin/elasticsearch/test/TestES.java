package net.wicp.tams.duckula.plugin.elasticsearch.test;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.es.Action;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.common.es.EsData.Builder;
import net.wicp.tams.common.es.EsObj;
import net.wicp.tams.common.es.bean.AliasesBean;
import net.wicp.tams.common.es.bean.IndexBean;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.bean.MappingBean.DataTypes;
import net.wicp.tams.common.es.bean.MappingBean.Dynamic;
import net.wicp.tams.common.es.bean.MappingBean.MappingBeanBuilder;
import net.wicp.tams.common.es.bean.MappingBean.Propertie;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;

@Slf4j
public class TestES {
	
	private void init() {
		Properties props = IOUtil.fileToProperties(new File(IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"),
				"/conf/duckula-plugin-elasticsearch.properties")));
		Conf.overProp(props);
	}
	
	@Test
	public void createClient() {
		RestHighLevelClient restHighLevelClient = ESClientOnlyOne.getInst().getESClient().getRestHighLevelClient();
		log.info(restHighLevelClient.toString());
		RestClient restClient = ESClientOnlyOne.getInst().getESClient().getRestClient();
		log.info(restClient.toString());
	}

	@Test
	public void createIndex() {		
		MappingBeanBuilder builder = MappingBean.builder();
		Map<String, Propertie> properties = new HashMap<>();
		Propertie pp=new Propertie();
		pp.setType(DataTypes.DOUBLE);
		properties.put("aaa", pp);
		builder.properties(properties);
		builder.dynamic(Dynamic.STRICT);
		Result createIndex = ESClientOnlyOne.getInst().getESClient().indexCreate("rjzjh", "rjzjh", 3, 0, builder.build());
		log.info(createIndex.getMessage());
	}

	@Test
	public void writeDoc() {
		JSONObject data = new JSONObject();
		data.put("aaa", 156.78d);
		Result writeDoc = ESClientOnlyOne.getInst().getESClient().docWrite("rjzjh", "rjzjh", data);
		log.info(writeDoc.getMessage());
	}

	@Test
	public void writeDocBatch_create() {
		List<EsData> esDatas = new ArrayList<EsData>();
		Builder newBuilder = EsData.newBuilder();
		newBuilder.setAction(Action.create);
		newBuilder.setIndex("rjzjh");
		newBuilder.setIsFast(true);
		newBuilder.setType("rjzjh");
		Map<String, String> newMap = CollectionUtil.newMap("aaa", "58"); 
		EsObj esData = EsObj.newBuilder().setId("abcd").putAllSource(newMap).build();
		newBuilder.addDatas(esData);
		esDatas.add(newBuilder.build());
		Result writeDocBatch = ESClientOnlyOne.getInst().getESClient().docWriteBatch(esDatas);
		log.info(writeDocBatch.getMessage());
	}

	@Test
	public void writeDocBatch_index() {
		List<EsData> esDatas = new ArrayList<EsData>();
		Builder newBuilder = EsData.newBuilder();
		newBuilder.setAction(Action.index);
		newBuilder.setIndex("rjzjh");
		newBuilder.setIsFast(true);
		newBuilder.setType("rjzjh");
		Map<String, String> newMap = CollectionUtil.newMap("aaa", "58");
		EsObj esData = EsObj.newBuilder().setId("abcd").putAllSource(newMap).build();
		newBuilder.addDatas(esData);
		esDatas.add(newBuilder.build());
		Result writeDocBatch = ESClientOnlyOne.getInst().getESClient().docWriteBatch(esDatas);
		log.info(writeDocBatch.getMessage());
	}

	@Test
	public void writeDocBatch_delete() {
		List<EsData> esDatas = new ArrayList<EsData>();
		Builder newBuilder = EsData.newBuilder();
		newBuilder.setAction(Action.delete);
		newBuilder.setIndex("rjzjh");
		newBuilder.setIsFast(true);
		newBuilder.setType("rjzjh");
		Map<String, String> newMap = CollectionUtil.newMap("aaa", "58");
		EsObj esData = EsObj.newBuilder().setId("abcd").putAllSource(newMap).build();
		newBuilder.addDatas(esData);
		esDatas.add(newBuilder.build());
		Result writeDocBatch = ESClientOnlyOne.getInst().getESClient().docWriteBatch(esDatas);
		log.info(writeDocBatch.getMessage());
	}

	/*****
	 * 局部更新 TODO
	 */
	@Test
	public void writeDocBatch_update() {
		List<EsData> esDatas = new ArrayList<EsData>();
		Builder newBuilder = EsData.newBuilder();
		newBuilder.setAction(Action.update);
		newBuilder.setIndex("rjzjh");
		newBuilder.setIsFast(true);
		newBuilder.setType("rjzjh");
		Map<String, String> newMap = CollectionUtil.newMap("aaa", "58");
		EsObj esData = EsObj.newBuilder().setId("abcd").putAllSource(newMap).build();
		newBuilder.addDatas(esData);
		esDatas.add(newBuilder.build());
		Result writeDocBatch = ESClientOnlyOne.getInst().getESClient().docWriteBatch(esDatas);
		log.info(writeDocBatch.getMessage());
	}

	@Test
	public void aliasCreate() {
		ESClientOnlyOne.getInst().getESClient().aliasCreate("a", "444444");
	}

	@Test
	public void indexReplace() {
		ESClientOnlyOne.getInst().getESClient().indexReplace("rjzjh", "a",false,"444444");
	}
	
	@Test
	public void queryIndex() throws IOException {
		List<IndexBean> queryIndex = ESClientOnlyOne.getInst().getESClient().queryIndex("rj*");
		log.info(""+queryIndex.size());
	}
	
	@Test
	public void queryAliases() {
		List<AliasesBean> queryIndex = ESClientOnlyOne.getInst().getESClient().queryAliases("44*");
		log.info(""+queryIndex.size());
	}
	
	@Test
	public void queryIndex_tc() throws IOException {
		 GetIndexResponse queryIndex_tc = ESClientOnlyOne.getInst().getESClient().queryIndex_tc("rjzjh");
		 log.info("a");
	}
	
	@Test
	public void queryM_tc() throws IOException {
		init();
		Map<String, DataTypes> queryMapping_tc = ESClientOnlyOne.getInst().getESClient().queryMapping_tc("file_type_test_v1", "_doc");
		 System.out.println(queryMapping_tc.get("aaa"));
		 log.info("a");
	}
	
	
	
	
}
