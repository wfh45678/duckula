package net.wicp.tams.duckula.plugin.kafka;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.others.kafka.IConsumer;
import net.wicp.tams.common.others.kafka.KafkaAssitInst;
import net.wicp.tams.common.others.kafka.KafkaConsumerGroup;
import net.wicp.tams.common.others.kafka.KafkaConsumerGroupB;
import net.wicp.tams.common.others.kafka.KafkaConsumerGroupS;
import net.wicp.tams.common.others.kafka.KafkaConsumerThread;
import net.wicp.tams.common.others.kafka.KafkaConsumerThreadB;

public class TestConnKafka {

	@Test
	public void conn() {
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		prop.put("acks", "1");
		prop.put("retries", 2147483647);
		prop.put("max.block.ms", 9223372036854775807L);
		prop.put("max.in.flight.requests.per.connection", 1);
		prop.put("linger.ms", 10);// 勇华 提高吞吐量
		prop.put("batch.size", 102400);
		prop.put("buffer.memory", 33554432);// 32M
		prop.put("compression.type", "none");
		prop.put("max.request.size", 1048576);
		prop.put("receive.buffer.bytes", 32768);
		prop.put("request.timeout.ms", 30000);
		prop.put("send.buffer.bytes", 131072);
		prop.put("connections.max.idle.ms", 540000);
		// link和batch不用加
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(prop);
		System.out.println("producer=" + producer);
	}

	@Test
	public void sendmsg() throws InterruptedException, ExecutionException, UnsupportedEncodingException {
		Properties props = IOUtil.fileToProperties(new File(
				IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf/duckula-plugin-kafka.properties")));
		Conf.overProp(props);
		KafkaProducer<String, byte[]> producer = KafkaAssitInst.getInst().getKafkaProducer(byte[].class);
		System.out.println("producer=" + producer);
		ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>("file_type_test",
				"abc".getBytes("UTF-8"));
		Future<RecordMetadata> send = producer.send(message);
		int partition = send.get().partition();
		System.out.println("partition===" + partition);
	}

	@Test
	public void pollmsg() throws InterruptedException {
		Properties props = IOUtil.fileToProperties(new File(
				IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf/duckula-plugin-kafka.properties")));
		Conf.overProp(props);
		IConsumer<byte[]> doConsumer = new IConsumer<byte[]>() {
			@Override
			public Result doWithRecords(List<ConsumerRecord<String, byte[]>> item) {
				return Result.getSuc();
			}
		};
		KafkaConsumerGroup<byte[]> group = new KafkaConsumerGroupB("group1", "test-demo", doConsumer,1);
		group.start();
		Thread.sleep(20 * 1000);
	}

	@Test
	public void pollmsg2() throws InterruptedException {
		Properties props = IOUtil.fileToProperties(new File(
				IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf/duckula-plugin-kafka.properties")));
		Conf.overProp(props);
		IConsumer<String> doConsumer = new IConsumer<String>() {
			@Override
			public Result doWithRecords(List<ConsumerRecord<String, String>> item) {
				return Result.getSuc();
			}
		};
		KafkaConsumerGroup<String> group = new KafkaConsumerGroupS("group6", "HelloWorld", doConsumer,1);
		group.start();
		Thread.sleep(120 * 1000);
	}

	@Test
	public void pollmsgthread() throws InterruptedException {
		Properties props = IOUtil.fileToProperties(new File(
				IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf/duckula-plugin-kafka.properties")));
		Conf.overProp(props);
		IConsumer<byte[]> doConsumer = new IConsumer<byte[]>() {
			@Override
			public Result doWithRecords(List<ConsumerRecord<String, byte[]>> item) {
				return Result.getSuc();
			}
		};
		KafkaConsumerThread<byte[]> group = new KafkaConsumerThreadB("group19", "test-demo", doConsumer);
		group.start(2);
		Thread.sleep(20 * 1000);
	}
}
