package net.wicp.tams.duckula.plugin.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.MurmurHash3;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.others.kafka.KafkaAssitInst;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;

@Slf4j
public class ReceiveKafka extends ReceiveAbs {

	private final KafkaProducer<String, byte[]> producer;
	private final Map<String, Integer> topicPartitionsMap = new HashMap<>();

	public ReceiveKafka(JSONObject paramObjs) {
		super(paramObjs);
		if (super.params == null || super.params.size() == 0) {
			log.warn(" 没有自定义的参数,将启用默认参数.");
		}
		for (String inputKey : params.keySet()) {
			props.put("common.others.kafka." + inputKey, params.get(inputKey));
		}
		Conf.overProp(props);
		producer = KafkaAssitInst.getInst().getKafkaProducer(byte[].class);
	}

	@Override
	public boolean receiveMsg(DuckulaPackage duckulaPackage, Rule rule, String splitKey) {
		String topic = rule.getItems().get(RuleItem.topic);
		if (!topicPartitionsMap.containsKey(topic)) {
			List<PartitionInfo> partiList = producer.partitionsFor(topic);
			topicPartitionsMap.put(topic, partiList.size());
		}
		int partitions = topicPartitionsMap.get(topic);
		final CountDownLatch latch = new CountDownLatch(duckulaPackage.getRowsNum());
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			Map<String, Map<String, String>> dataMap = pluginAssit.getAllData(duckulaPackage, i);
			String splitValue = dataMap
					.get(dataMap.containsKey(pluginAssit.colAfter) ? pluginAssit.colAfter : pluginAssit.colBefore)
					.get(splitKey);
			final String key = String.format("%s|%s|%s", splitValue, duckulaPackage.getEventTable().getDb(),
					duckulaPackage.getEventTable().getTb());
			try {
				ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic,
						partitions < 2 ? 0 : Math.abs(partition(splitValue, partitions)), key,
						JSONObject.toJSONString(dataMap).getBytes("UTF-8"));
				producer.send(message, new Callback() {
					@Override
					public void onCompletion(RecordMetadata ret, Exception exception) {
						if (exception != null) {// 异常不管，kafka自己有重试机制
							log.error("TimeoutException: Batch Expired,send again:{}", key);
						} else {
							latch.countDown();
						}
					}
				});
			} catch (Exception e) {
				log.error(String.format("send error,first colvalue:[%s]", key), e);
				throw new IllegalAccessError("发送消息时异常");
			}
		}
		try {
			boolean retvalue = latch.await(60, TimeUnit.SECONDS);
			return retvalue;
		} catch (InterruptedException e) {
			log.error("发送中断", e);
			return false;
		}
	}

	@Override
	public boolean receiveMsg(List<SingleRecord> data, Rule rule) {
		String topic = rule.getItems().get(RuleItem.topic);
		if (!topicPartitionsMap.containsKey(topic)) {
			List<PartitionInfo> partiList = producer.partitionsFor(topic);
			topicPartitionsMap.put(topic, partiList.size());
		}
		int partitions = topicPartitionsMap.get(topic);
		final CountDownLatch latch = new CountDownLatch(data.size());
		for (final SingleRecord singleRecord : data) {
			final String key = String.format("%s|%s|%s", singleRecord.getKey(), singleRecord.getDb(),
					singleRecord.getTb());

			try {
				ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic,
						partitions < 2 ? 0 : Math.abs(partition(singleRecord.getKey(), partitions)), key,
						singleRecord.getData());

				producer.send(message, new Callback() {
					@Override
					public void onCompletion(RecordMetadata ret, Exception exception) {
						if (exception != null) {// 异常不管，kafka自己有重试机制
							log.error("TimeoutException: Batch Expired,send again:{}", key);
						} else {
							latch.countDown();
						}
					}
				});
			} catch (Exception e) {
				log.error(String.format("send error,first colvalue:[%s]", key), e);
				throw new IllegalAccessError("发送消息时异常");
			}
		}
		try {
			return latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("发送中断", e);
			return false;
		}
	}

	private int partition(String value, int partitions) {
		long valueL = 0;
		if (StringUtil.isNull(value)) {// 防止第1列或分库分表键的值为空值的情况
			return 0;
		}
		try {
			valueL = Long.parseLong(value);
		} catch (Exception e) {
			valueL = MurmurHash3.murmurhash3_x86_32(value, 0, value.length(), 25342);// 种子25342
																						// //有可能产生负数如"f9ce4495-2b42-4839-8d9a-3ba03c7c1ce8"
		}
		return (int) valueL % partitions;
	}

	@Override
	public boolean isSync() {
		return true;// 因为已在具体插件（本类）中实现异步转同步逻辑，所以在主程序中保持同步
	}

}
