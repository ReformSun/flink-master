package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

public class TestMain6 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		Properties propertie = new Properties();
		propertie.setProperty("input-topic", "monitorBlocklyQueueKeyJson");
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		StreamQueryConfig qConfig = tableEnv.queryConfig();


		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a", Types.STRING).field("b", Types.INT).field("rtime", Types.SQL_TIMESTAMP).build()).withProctimeAttribute("rtime");

		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		tableEnv.registerTableSource("kafkasource1", kafkaTableSource);
		Table sqlResult = tableEnv.sqlQuery("SELECT a,SUM(b) as cnt FROM kafkasource1 GROUP BY a");


		tableEnv.registerTable("table1",sqlResult);
		Table sqlResult2 = tableEnv.sqlQuery("SELECT * FROM table1");

		DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(sqlResult, Row.class, qConfig);
		DataStream<Tuple2<Boolean, Row>> stream2 = tableEnv.toRetractStream(sqlResult2, Row.class, qConfig);


		stream.addSink(new RichSinkFunction<Tuple2<Boolean, Row>>() {
			@Override
			public void invoke(Tuple2<Boolean, Row> value) throws Exception {
				writerFile(value.getField(1).toString());
			}

			public  void writerFile(String s) throws IOException {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(s);
				}
			}
		});

		stream2.addSink(new RichSinkFunction<Tuple2<Boolean, Row>>() {
			@Override
			public void invoke(Tuple2<Boolean, Row> value) throws Exception {
				writerFile(value.getField(1).toString());
			}

			public  void writerFile(String s) throws IOException {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test1.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(s);
				}
			}
		});


		sEnv.execute();
	}
}
