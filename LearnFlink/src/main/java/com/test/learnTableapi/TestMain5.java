package com.test.learnTableapi;

import com.test.defineFunction.Test;
import com.test.defineFunction.Test2;
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
import org.apache.flink.table.api.java.Slide;
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
		propertie.setProperty("input-topic", "monitorBlocklyQueueKeyJson2");
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a", Types.STRING).field("d",Types.INT).field("b", Types.INT).field("rtime", Types.SQL_TIMESTAMP).build()).withRowtimeAttribute("rtime", new ExistingField("rtime"),new BoundedOutOfOrderTimestamps(0L));

		KafkaTableSource kafkaTableSource = jsonTableSourceBuilder.build();
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);

		Table sqlResult = tableEnv.sqlQuery("SELECT SUM(b) > 10 as m, TUMBLE_START(rtime, INTERVAL '10' SECOND) FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL '10' SECOND)");
		Table sqlResul2 = tableEnv.sqlQuery("SELECT SUM(d) > 20 as m, TUMBLE_START(rtime, INTERVAL '10' SECOND) FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL '10' SECOND)");
		Table sqlResul3 = tableEnv.sqlQuery("SELECT SUM(d) > 5 as m, TUMBLE_START(rtime, INTERVAL '10' SECOND) FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL '10' SECOND)");

//		Table table = sqlResult.union(sqlResul2);

		tableEnv.registerTable("a",sqlResult);
		tableEnv.registerTable("b",sqlResul2);
		tableEnv.registerTable("c",sqlResul3);

//		Table table = tableEnv.sqlQuery("SELECT * FROM( SELECT m FROM a) UNION ALL (SELECT m FROM b) UNION ALL (SELECT m FROM c)");
		Table table = tableEnv.sqlQuery("SELECT a.m as m,b.m as k,c.m as j FROM a,b,c");


//		tableEnv.registerTable("table1",sqlResult);
//		Table sqlResult2 = tableEnv.sqlQuery("SELECT * FROM table1");
//		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.INT,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(table,Row.class,qConfig);

		stream.addSink(new RichSinkFunction<Row>() {
			@Override
			public void invoke(Row value) throws Exception {
				for (int i = 0; i < value.getArity(); i++) {
					Boolean object = (Boolean)value.getField(i);
					System.out.println(i +  "dddd" + object);
					writerFile(i +  "dddd" + object);
				}


			}

			public  void writerFile(String s) throws IOException {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(s);
				}
			}
		});

//		Table table = tableEnv.fromDataStream(stream,"b,c.rowtime");
//		tableEnv.registerTable("table1",table);
//		tableEnv.registerFunction("test",new Test2());
//		tableEnv.registerFunction("test1",new Test());
//
////		Table sqlResult2 = table.window(Slide.over("20.second").every("10.second").on("c").as("w")).groupBy("w,b").select("test(c)");
//		Table sqlResult2 = tableEnv.sqlQuery("SELECT test(c,b) FROM table1 GROUP BY HOP(c,INTERVAL '10' SECOND,INTERVAL '20' SECOND),c,b");
//		DataStream<Row> stream2 = tableEnv.toAppendStream(sqlResult2,Row.class,qConfig);
//
//
//		stream2.addSink(new RichSinkFunction<Row>() {
//			@Override
//			public void invoke(Row value) throws Exception {
//				writerFile(value.toString());
//			}
//
//			public  void writerFile(String s) throws IOException {
//				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test1.txt");
//				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
//					writer.newLine();
//					writer.write(s);
//				}
//			}
//		});
//		Table table = tableEnv.fromDataStream(stream,"b,c.rowtime");
//		tableEnv.registerTable("table1",table);
//		tableEnv.registerFunction("test",new Test2());
//		tableEnv.registerFunction("test1",new Test());
//
//		Table sqlResult2 = table.window(Slide.over("20.second").every("10.second").on("c").as("w")).groupBy("w,b,c").select("c,b");
////		Table sqlResult2 = tableEnv.sqlQuery("SELECT c,b FROM table1 GROUP BY HOP(c,INTERVAL '10' SECOND,INTERVAL '20' SECOND),c,b");
//		DataStream<Row> stream2 = tableEnv.toAppendStream(sqlResult2,Row.class,qConfig);
//
//		stream2.addSink(new RichSinkFunction<Row>() {
//			@Override
//			public void invoke(Row value) throws Exception {
//				writerFile(value.toString());
//			}
//
//			public  void writerFile(String s) throws IOException {
//				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test1.txt");
//				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
//					writer.newLine();
//					writer.write(s);
//				}
//			}
//		});

		sEnv.execute();
	}
}
