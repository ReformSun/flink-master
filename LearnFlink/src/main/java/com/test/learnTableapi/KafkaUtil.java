package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;

import java.util.Properties;

public class KafkaUtil {
	public static KafkaTableSource getKafkaTableSource(String topic,TableSchema tableSchema,String rowTimeName){
		Properties propertie = new Properties();
		propertie.setProperty("input-topic",topic);
//		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092,172.31.24.36:9092");
		propertie.setProperty("bootstrap.servers", "172.31.35.58:9092");
//		propertie.setProperty("bootstrap.servers", "172.31.24.36:9092");
		propertie.setProperty("group.id", "serverCollector");
		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchema).withRowtimeAttribute(rowTimeName, new ExistingField(rowTimeName),new BoundedOutOfOrderTimestamps(30000L));
		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		return kafkaTableSource;
	}

	public static KafkaTableSource getKafka11TableSource(String topic,TableSchema tableSchema,String rowTimeName){
		Properties propertie = new Properties();
		propertie.setProperty("input-topic",topic);
//		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092,172.31.24.36:9092");
		propertie.setProperty("bootstrap.servers", "172.31.35.58:9092");
//		propertie.setProperty("bootstrap.servers", "172.31.24.36:9092");
		propertie.setProperty("group.id", "serverCollector");
		propertie.put("enable.auto.commit", "true");
		propertie.put("auto.commit.interval.ms", "10000");
		propertie.put("auto.offset.reset", "earliest");
		propertie.put("session.timeout.ms", "30000");
		propertie.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propertie.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		Kafka011JsonTableSource.Builder jsonTableSourceBuilder = Kafka011JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchema).withRowtimeAttribute(rowTimeName, new ExistingField(rowTimeName),new BoundedOutOfOrderTimestamps(30000L));
		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		return kafkaTableSource;
	}

}
