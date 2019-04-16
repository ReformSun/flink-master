package com.test.learnWindows;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

public class KafkaUtil {
	public static FlinkKafkaConsumer09 getKafkaConsumer09Source(String topic){
		Properties propertie = new Properties();
		propertie.setProperty("input-topic",topic);
		propertie.setProperty("bootstrap.servers", "172.31.35.58:9092");
//		propertie.setProperty("bootstrap.servers", "172.31.24.36:9092");
		propertie.setProperty("group.id", "serverCollector");
		FlinkKafkaConsumer09 flinkKafkaConsumer09 = new FlinkKafkaConsumer09(
			propertie.getProperty("input-topic"),
			new StreamModelSchema(),
			propertie);
		return flinkKafkaConsumer09;
	}

	public static FlinkKafkaConsumer010 getKafkaConsumer010Source(String topic){
		Properties propertie = new Properties();
		propertie.setProperty("input-topic",topic);
		propertie.setProperty("bootstrap.servers", "172.31.35.58:9092");
//		propertie.setProperty("bootstrap.servers", "172.31.24.36:9092");
		propertie.setProperty("group.id", "serverCollector");
		FlinkKafkaConsumer010 flinkKafkaConsumer010 = new FlinkKafkaConsumer010(
			propertie.getProperty("input-topic"),
			new StreamModelSchema(),
			propertie);
		return flinkKafkaConsumer010;
	}

}
