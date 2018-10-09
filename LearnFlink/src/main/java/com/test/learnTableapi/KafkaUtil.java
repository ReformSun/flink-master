package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
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
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchema).withRowtimeAttribute(rowTimeName, new ExistingField(rowTimeName),new BoundedOutOfOrderTimestamps(30000L));
		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		return kafkaTableSource;
	}

}
