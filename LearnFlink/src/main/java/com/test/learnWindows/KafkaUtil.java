package com.test.learnWindows;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;

import java.util.Properties;

public class KafkaUtil {
	public static FlinkKafkaConsumer09 getKafkaTableSource(String topic){
		Properties propertie = new Properties();
		propertie.setProperty("input-topic",topic);
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		FlinkKafkaConsumer09 flinkKafkaConsumer09 = new FlinkKafkaConsumer010<>(
			propertie.getProperty("input-topic"),
			new StreamModelSchema(),
			propertie);
		return flinkKafkaConsumer09;
	}

}
