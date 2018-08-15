package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

import java.util.Properties;

public class TestMain5 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		Properties propertie = new Properties();
		propertie.setProperty("input-topic","monitorBlocklyQueueKey6");
		propertie.setProperty("bootstrap.servers","172.31.24.30:9092");
		propertie.setProperty("group.id","serverCollector");
		StreamQueryConfig qConfig = tableEnv.queryConfig();


		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));

		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a", Types.STRING).field("b",Types.INT).field("rtime",Types.SQL_TIMESTAMP).build()).withProctimeAttribute("rtime");

		KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);
		Table sqlResult = tableEnv.sqlQuery("SELECT SUM(b) FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL '10' SECOND)");

		CsvTableSink csvTableSink = new CsvTableSink("file:///Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);
		sqlResult.writeToSink(csvTableSink);

		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,Row.class,qConfig);

		sEnv.execute();
	}
}
