package net.wicp.tams.duckula.kafka.consumer.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcData;
import net.wicp.tams.common.apiext.jdbc.JdbcDatas;
import net.wicp.tams.common.apiext.jdbc.JdbcDatas.Builder;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.DbInstance;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Slf4j
public class ConsumerMysqlImpl extends ConsumerAbs<JdbcDatas.Builder> {

	private Map<Rule, DataSource> dsmap = new HashMap<>();

	public ConsumerMysqlImpl(Consumer consumer) {
		super(consumer);
	}

	private Map<String, Rule> ruleMaToDb = new HashMap<>();// 序列化后用的ruleMap

	@Override
	public Builder packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule) {
		if (dsmap.get(rule) == null) {
			synchronized (ConsumerMysqlImpl.class) {
				if (dsmap.get(rule) == null) {
					Properties copyProperties = Conf.copyProperties();
					String dbInstanceId = rule.getItems().get(RuleItem.dbinstanceid);
					DbInstance dbInstance = JSONObject.toJavaObject(
							ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(dbInstanceId)), DbInstance.class);
					copyProperties.put("host", dbInstance.getUrl());
					copyProperties.put("port", dbInstance.getPort());
					copyProperties.put("username", dbInstance.getUser());
					copyProperties.put("password", dbInstance.getPwd());
					// 防止com.mysql.jdbc.exceptions.jdbc4.CommunicationsException: Communications
					// link failure
					copyProperties.put("urlparam",
							"useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false");
					// 打开pschche
					copyProperties.put("maxPoolPreparedStatementPerConnectionSize", 200);
					copyProperties.put("filters", "stat");
					DataSource newDataSource = DruidAssit.getDataSourceNoConf("consumer_"+dbInstanceId, copyProperties);
					dsmap.put(rule, newDataSource);
				}
			}
		}
		String dbtb = rule.getItems().get(RuleItem.dbtb);
		if (ruleMaToDb.get(dbtb) == null) {
			ruleMaToDb.put(dbtb, rule);
		}
		Builder newBuilder = JdbcDatas.newBuilder();
		newBuilder.addAllCols(duckulaEvent.getColsList());
		String[] dbtbAry = dbtb.split("\\.");
		newBuilder.setDb(dbtbAry[0]);
		newBuilder.setTb(dbtbAry[1]);
		newBuilder.setOptTypeValue(duckulaEvent.getOptTypeValue());
		String[] keys = primarysMap.get(String.format("%s.%s", duckulaEvent.getDb(), duckulaEvent.getTb()));
		newBuilder.addAllKeys(Arrays.asList(keys));
		for (int i = 0; i < duckulaEvent.getColsCount(); i++) {
			newBuilder.putType(duckulaEvent.getCols(i), duckulaEvent.getColsType(i).name());
		}
		// 值
		net.wicp.tams.common.apiext.jdbc.JdbcData.Builder jdbcDataBuild = JdbcData.newBuilder();
		jdbcDataBuild.putAllValue(datamap);
		newBuilder.addDatas(jdbcDataBuild);
		return newBuilder;
	}

	@Override
	public Result doSend(List<Builder> builders) {
		Builder sendBuilder = null;
		String curkey = null;
		for (Builder builder : builders) {
			String tempkey = String.format("%s.%s:%s", builder.getDb(), builder.getTb(), builder.getOptType().name());
			if (curkey == null) {
				sendBuilder = builder;
				curkey = tempkey;
				continue;
			}
			if (curkey.equals(tempkey)) {
				sendBuilder.addAllDatas(builder.getDatasList());
			} else {
				send(sendBuilder);
				sendBuilder = builder;
				curkey = tempkey;
				continue;
			}
		}
		if (sendBuilder != null) {
			send(sendBuilder);
		}
		return Result.getSuc();
	}

	private void send(Builder sendBuilder) {
		try {
			String key = String.format("%s.%s", sendBuilder.getDb(), sendBuilder.getTb());
			DataSource dataSource = dsmap.get(ruleMaToDb.get(key));
			Connection connection = dataSource.getConnection();
			Result temp = MySqlAssit.dataChange(connection, sendBuilder.build());
			if (!temp.isSuc()) {
				log.error("同步错误,原因：{}", temp.getMessage());
				LoggerUtil.exit(JvmStatus.s15);
			}
			connection.close();
		} catch (SQLException e) {
			log.error("发送失败", e);
			LoggerUtil.exit(JvmStatus.s15);
		}
	}

	@Override
	public boolean checkDataNull(Builder data) {
		return data.getDatasCount() == 0;
	}
}
