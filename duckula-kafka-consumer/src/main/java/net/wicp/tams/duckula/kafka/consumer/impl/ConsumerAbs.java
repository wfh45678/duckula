package net.wicp.tams.duckula.kafka.consumer.impl;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.TimeAssist;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectExceptionRuntime;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.common.others.kafka.IConsumer;
import net.wicp.tams.duckula.client.DuckulaAssit;
import net.wicp.tams.duckula.client.DuckulaIdempotentAssit;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEventIdempotent;
import net.wicp.tams.duckula.client.Protobuf3.IdempotentEle;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.PluginType;
import net.wicp.tams.duckula.kafka.consumer.MainConsumer;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer;

@Slf4j
public abstract class ConsumerAbs<T> implements IConsumer<byte[]> {
	protected final Consumer consumer;
	protected final IBusiConsumer<T> busiEs;
	protected Connection connection = DruidAssit.getConnection();// TODO 每线程一个连接
	private Map<Rule, PreparedStatement> statMap = new HashMap<>();
	protected Map<String, String[]> primarysMap = new HashMap<>();
	protected Map<String, String[]> colsMap = new HashMap<>();

	protected static final org.slf4j.Logger errorlog = org.slf4j.LoggerFactory.getLogger("errorBinlog");// 需要跳过的错误数据。

	protected final boolean isIde;

	@SuppressWarnings("unchecked")
	public ConsumerAbs(Consumer consumer) {
		this.consumer = consumer;
		if (StringUtil.isNotNull(consumer.getBusiPlugin())) {
			try {
				String plugDir = consumer.getBusiPlugin().replace(".tar", "");
				String plugDirSys = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDir(false), plugDir);
				ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
				Plugin plugin = pluginAssit.newPlugin(plugDirSys,
						"net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer", classLoader,
						"net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer",
						"net.wicp.tams.duckula.plugin.receiver.ReceiveAbs");
				Thread.currentThread().setContextClassLoader(plugin.getLoad().getClassLoader());// 需要加载前设置好classload
				this.busiEs = (IBusiConsumer<T>) plugin.newObject();
				log.info("the busi plugin is sucess");
			} catch (Throwable e) {
				log.error("组装业务插件失败", e);
				LoggerUtil.exit(JvmStatus.s9);
				throw new RuntimeException("组装业务插件失败");
			}
		} else {
			busiEs = null;
		}
		Task task = ZkUtil.buidlTask(consumer.getTaskOnlineId());
		isIde = task.getSenderEnum().isIdempotent();
		if (consumer.getMiddlewareType() != null && StringUtil.isNotNull(consumer.getMiddlewareInst())) {// 中间件集群相关配置
			Properties props = ConfUtil.configMiddleware(consumer.getMiddlewareType(), consumer.getMiddlewareInst());
			Conf.overProp(props);
		}
	}

	public abstract T packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule);

	public abstract Result doSend(List<T> datas);

	public abstract boolean checkDataNull(T data);// 判断数据是否为空,true:空 false:不为空

	@Override
	public Result doWithRecords(List<ConsumerRecord<String, byte[]>> consumerRecords) {
		List<T> datas = new ArrayList<>();
		for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
			Rule rule = null;
			if (!isIde) {
				DuckulaEvent duckulaEvent = null;
				try {
					duckulaEvent = DuckulaAssit.parse(consumerRecord.value());
				} catch (InvalidProtocolBufferException e1) {
					log.error("解析失败", e1);
					continue;// 不处理此数据
				}
				duckulaEventToDatas(datas, duckulaEvent, rule);
			} else {
				DuckulaEventIdempotent duckulaEventIdempotent = null;
				try {
					duckulaEventIdempotent = DuckulaIdempotentAssit.parse(consumerRecord.value());
				} catch (InvalidProtocolBufferException e1) {
					log.error("解析失败", e1);
					continue;// 不处理此数据
				}
				Builder duckulaEventBuilder = DuckulaEvent.newBuilder();
				duckulaEventBuilder.setDb(duckulaEventIdempotent.getDb());
				duckulaEventBuilder.setTb(duckulaEventIdempotent.getTb());
				duckulaEventBuilder.setOptType(duckulaEventIdempotent.getOptType());
				duckulaEventBuilder.addAllCols(duckulaEventIdempotent.getKeyNamesList());
				duckulaEventBuilder.addAllColsType(duckulaEventIdempotent.getKeyTypesList());
				duckulaEventBuilder.setIsError(true);
				for (IdempotentEle dempotentEle : duckulaEventIdempotent.getValuesList()) {
					Builder tempEventBuild = duckulaEventBuilder.clone();
					for (int i = 0; i < dempotentEle.getKeyValuesCount(); i++) {
						String keyValues = dempotentEle.getKeyValues(i);
						if (duckulaEventIdempotent.getOptType() == OptType.delete) {
							tempEventBuild.putBefore(duckulaEventBuilder.getCols(i), keyValues);
						} else {
							tempEventBuild.putAfter(duckulaEventBuilder.getCols(i), keyValues);
						}
					}
					duckulaEventToDatas(datas, tempEventBuild.build(), rule);
				}
			}

		}
		if (datas.size() == 0) {// 没有可用的数据，有可能是合理跳过了某些数据
			return Result.getSuc();
		} else {
			List<T> removeList = new ArrayList<>();
			for (T data : datas) {
				if (checkDataNull(data)) {
					removeList.add(data);
				}
			}
			if (datas.size() == removeList.size()) {// 没有可用的数据，有可能是合理跳过了某些数据
				return Result.getSuc();
			}
			datas.removeAll(removeList);
		}
		// 插件化
		if (busiEs != null) {
			try {
				this.busiEs.doBusi(datas);
			} catch (Throwable e) {
				log.error("业务处理失败", e);// TODO
				LoggerUtil.exit(JvmStatus.s15);
			}
		}

		// 20190624 增加重试机制
		while (true) {
			try {
				Result sendResult = doSend(datas);
				if (sendResult.isSuc()) {
					MainConsumer.metric.counter_send_es.inc(datas.size());
					log.info("this batch sended num：{},all num:{}, max record dept:{},offiset：{}", datas.size(),
							MainConsumer.metric.counter_send_es.getCount(),
							consumerRecords.get(consumerRecords.size() - 1).partition(),
							consumerRecords.get(consumerRecords.size() - 1).offset());
					TimeAssist.reDoWaitInit("tams-consumer");
					return Result.getSuc();
				} else {
					log.error("发送失败，原因:{}", sendResult.getMessage());
					boolean reDoWait = TimeAssist.reDoWait("tams-consumer", 8);
					if (reDoWait) {
						LoggerUtil.exit(JvmStatus.s15);// 达到了最大值，退出
						return sendResult;
					}
				}
			} catch (Throwable e) {
				log.error("发送异常", e);
				boolean reDoWait = TimeAssist.reDoWait("tams-consumer", 8);
				if (reDoWait) {
					LoggerUtil.exit(JvmStatus.s15);// 达到了最大值，退出
					throw new ProjectExceptionRuntime(ExceptAll.duckula_es_batch);
				}
			}
		}
	}

	public void duckulaEventToDatas(List<T> datas, DuckulaEvent duckulaEvent, Rule rule) {
		try {
			if (rule == null) {
				rule = findReule(consumer, duckulaEvent.getDb(), duckulaEvent.getTb());
				if (rule == null) {
					return;
				}
			}
			String keymapkey = String.format("%s.%s", duckulaEvent.getDb(), duckulaEvent.getTb());
			if (primarysMap.get(keymapkey) == null) {
				String keyColName = rule.getItems().get(RuleItem.key);
				String[] primarys;
				if (StringUtil.isNull(keyColName)) {
					primarys = MySqlAssit.getPrimary(connection, duckulaEvent.getDb(), duckulaEvent.getTb());
				} else {
					primarys = keyColName.split(",");
				}
				primarysMap.put(keymapkey, primarys);
			}
			if (colsMap.get(keymapkey) == null) {
				String[][] cols = MySqlAssit.getCols(connection, duckulaEvent.getDb(), duckulaEvent.getTb(),
						YesOrNo.no);
				colsMap.put(keymapkey, cols[0]);
			}

			Serializable[] keyValues = new Serializable[primarysMap.get(keymapkey).length];
			for (int i = 0; i < keyValues.length; i++) {
				keyValues[i] = DuckulaAssit.getValue(duckulaEvent, primarysMap.get(keymapkey)[i]);
			}
			String idStr = CollectionUtil.arrayJoin(keyValues, "-");
			Map<String, String> datamap = new HashMap<>();
			if (duckulaEvent.getOptType() == OptType.delete) {
				for (int i = 0; i < keyValues.length; i++) {
					datamap.put(primarysMap.get(keymapkey)[i], String.valueOf(keyValues[i]));
				}
			} else if (duckulaEvent.getIsError()) {
				StringBuilder build = new StringBuilder();
				build.append("select * from " + keymapkey + " where ");
				for (int i = 0; i < primarysMap.get(keymapkey).length; i++) {
					build.append(String.format(" %s=?", primarysMap.get(keymapkey)[i]));
				}
				Connection connection = DruidAssit.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(build.toString());
				JdbcAssit.setPreParam(preparedStatement, keyValues);
				ResultSet rs = preparedStatement.executeQuery();
				if (isIde) {
					if (rs.next()) {
						for (String colName : colsMap.get(keymapkey)) {
							try {
								String valuestr = rs.getString(colName);
								datamap.put(colName, valuestr);
							} catch (Exception e) {
								log.error("no colName:" + colName, e);
							}
						}
					} else {
						log.error("没有找到ID:{}", idStr);
						return;
					}
					// List<Map<String, String>> rsToMap = JdbcAssit.rsToMap(rs);
					// rs.last(); // 20191202 Operation not allowed after ResultSet closed
					// https://www.cnblogs.com/haore147/p/3617767.html
					// if (CollectionUtils.isNotEmpty(rsToMap)) {
					// datamap.putAll(rsToMap.get(0));
					// } else {
					// log.error("没有找到ID:{}", idStr);
					// return;
					// }
				} else {
					if (rs.next()) {
						for (String colName : duckulaEvent.getColsList()) {
							String valuestr = rs.getString(colName);
							datamap.put(colName, valuestr);
						}
					} else {
						log.error("没有找到ID:{}", idStr);
						return;
					}
				}
				try {
					rs.close();
					preparedStatement.close();		
					connection.close();
				} catch (Exception e) {
					log.error("关闭es失败", e);
				}
			} else {
				for (String colName : duckulaEvent.getColsList()) {
					String valuestr = DuckulaAssit.getValueStr(duckulaEvent, colName);
					datamap.put(colName, valuestr);
				}
			}
			CollectionUtil.filterNull(datamap, 1);
			T packObj = packObj(duckulaEvent, datamap, rule);
			if (packObj != null) {// 没有错误，有些错误需要跳过
				datas.add(packObj);
			}
		} catch (Exception e) {
			log.error("组装失败", e);
			LoggerUtil.exit(JvmStatus.s15);
			throw new ProjectExceptionRuntime(ExceptAll.duckula_es_formate);
		}
	}

	private Map<String, Rule> ruleMap = new HashMap<>();// 原始数据对应的规则

	private Rule findReule(Consumer consumer, String db, String tb) {
		String key = String.format("%s.%s", db, tb);
		if (ruleMap.get(key) == null) {
			Rule findRule = consumer.findRule(db, tb);
			ruleMap.put(key, findRule);
		}
		return ruleMap.get(key);
	}
}
