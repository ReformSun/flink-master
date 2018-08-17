package com.test.learnTableapi;

import com.test.sink.CustomPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import scala.collection.mutable.ArrayBuffer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.Properties;

public class TestMain5 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		Properties propertie = new Properties();
		propertie.setProperty("input-topic", "monitorBlocklyQueueKeyJson");
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a", Types.STRING).field("b", Types.INT).field("rtime", Types.SQL_TIMESTAMP).build()).withRowtimeAttribute("rtime", new ExistingField("rtime"),new BoundedOutOfOrderTimestamps(30000L));

		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);

		Table sqlResult = tableEnv.sqlQuery("SELECT a,SUM(b) as ba, TUMBLE_START(rtime, INTERVAL '10' SECOND) as ttime FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL '10' SECOND),a");

		tableEnv.registerTable("table1",sqlResult);
		Table sqlResult2 = tableEnv.sqlQuery("SELECT * FROM table1");



		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,Row.class,qConfig);
		DataStream<Row> stream2 = tableEnv.toAppendStream(sqlResult2,Row.class,qConfig);


		Table table = tableEnv.fromDataStream(stream,"a,b,c.rowtime");


//		stream.addSink(new RichSinkFunction<Row>() {
//			@Override
//			public void invoke(Row value) throws Exception {
//				writerFile(value.toString());
//			}
//
//			public  void writerFile(String s) throws IOException {
//				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
//				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
//					writer.newLine();
//					writer.write(s);
//				}
//			}
//		});

		stream2.addSink(new RichSinkFunction<Row>() {
			@Override
			public void invoke(Row value) throws Exception {
				writerFile(value.toString());
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
