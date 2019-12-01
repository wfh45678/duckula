package net.wicp.tams.duckula.plugin.kafka;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.others.kafka.KafkaAssitInst;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;

@Slf4j
public class ReceiveKafkaIdempotent extends ReceiveAbs {

	private final KafkaProducer<String, byte[]> producer;

	public ReceiveKafkaIdempotent(JSONObject paramObjs) {
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
	public boolean receiveMsg(List<SingleRecord> data, Rule rule) {
		String topic = rule.getItems().get(RuleItem.topic);
		SingleRecord singleRecord = data.get(0);// 只有一条记录
		final String key = String.format("%s|%s|%s",
				new SimpleDateFormat("MMddHHmmss").format(System.currentTimeMillis()), singleRecord.getDb(),
				singleRecord.getTb());
		try {
			ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, key,
					singleRecord.getData());
			producer.send(message, new Callback() {
				@Override
				public void onCompletion(RecordMetadata ret, Exception exception) {
					if (exception != null) {// 异常不管，kafka自己有重试机制
						log.error("TimeoutException: Batch Expired,send again:{}", key);
					}
				}
			});
		} catch (Exception e) {
			log.error(String.format("send error,first colvalue:[%s]", key), e);
			throw new IllegalAccessError("发送消息时异常");
		}
		return true;
	}

	@Override
	public boolean isSync() {
		return true;
	}

	@Override
	public boolean receiveMsg(DuckulaPackage duckulaPackage, Rule rule, String splitKey) {
		if (duckulaPackage.getRowsNum() == 0) {
			return true;
		}
		String topic = rule.getItems().get(RuleItem.topic);
		JSONObject data = new JSONObject();
		data.put("db", duckulaPackage.getEventTable().getDb());
		data.put("tb", duckulaPackage.getEventTable().getDb());
		data.put("OptType", duckulaPackage.getEventTable().getOptType().name());
		data.put("commitTime", duckulaPackage.getEventTable().getCommitTime());
		String[][] orivalues = duckulaPackage.getEventTable().getOptType() == OptType.delete ? duckulaPackage.getBefores() : duckulaPackage.getAfters();
		String[] cols = duckulaPackage.getEventTable().getCols();
		int keyIndex[] = new int[0];
		if (StringUtil.isNotNull(splitKey)) {
			String[] keyCols = splitKey.split(",");
			keyIndex = new int[keyCols.length];
			for (int i = 0; i < keyCols.length; i++) {
				keyIndex[i] = ArrayUtils.indexOf(cols, keyCols[i]);
			}
		}
		
		JSONArray ary = new JSONArray();
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			Map<String, Map<String, String>> dataMap = pluginAssit.getAllData(duckulaPackage, i);			
			JSONObject tempData = new JSONObject();
			for (int keyindex : keyIndex) {				
				tempData.put(cols[keyindex], orivalues[i][keyindex]);
			}
			ary.add(tempData);
		}
		data.put("data", ary);
		final String key = String.format("%s|%s", duckulaPackage.getEventTable().getDb(),
				duckulaPackage.getEventTable().getTb());
		try {
			ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, key,
					JSONObject.toJSONString(data).getBytes("UTF-8"));
			producer.send(message, new Callback() {
				@Override
				public void onCompletion(RecordMetadata ret, Exception exception) {
					if (exception != null) {// 异常不管，kafka自己有重试机制
						log.error("TimeoutException: Batch Expired,send again:{}", key);
					}
				}
			});
		} catch (Exception e) {
			log.error(String.format("send error,first colvalue:[%s]", key), e);
			throw new IllegalAccessError("发送消息时异常");
		}
		return true;
	}

}
