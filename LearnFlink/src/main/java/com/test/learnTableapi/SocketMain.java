package com.test.learnTableapi;

import com.test.customTableSource.SocketTableSource;
import com.test.defineFunction.Test;
import com.test.defineFunction.Test2;
import com.test.map.TestMap;
import com.test.util.URLUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.scala.row;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

public class SocketMain {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		SocketTableSource socketTableSource = new SocketTableSource();
		tableEnv.registerTableSource("kafkasource", socketTableSource);

		Table sqlResult = tableEnv.sqlQuery("SELECT SUM(countt) as bb,TUMBLE_START(rtime, INTERVAL '10' SECOND) as ttime FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL" +
			" '10' " +
			"SECOND)" +
			"," +
			"username");



		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.INT,Types.SQL_TIMESTAMP);

		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,rowTypeInfo,qConfig);
		Table table = tableEnv.fromDataStream(stream,"b,c");
		tableEnv.registerTable("table1",table);
		tableEnv.registerTable("table2",sqlResult);
		tableEnv.registerFunction("test",new Test());


//		Table table1 = tableEnv.sqlQuery("select * from table1 INNER JOIN table2 ON table1.b = table2.bb");
		Table table1 = tableEnv.sqlQuery("select * from table1 where b > 0");
		DataStream<Row> stream2 = tableEnv.toAppendStream(table1,Row.class,qConfig);


		stream2.addSink(new RichSinkFunction<Row>() {
			@Override
			public void invoke(Row value) throws Exception {
				for (int i = 0; i < value.getArity(); i++) {
					System.out.println(value.getField(i));
				}

				writerFile(value.toString());
			}

			public  void writerFile(String s) throws IOException {
				Path logFile = Paths.get(URLUtil.baseUrl +"test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(s);
				}
			}
		});




//		tableEnv.registerFunction("test",new Test2());
//		Table sqlResult2 = tableEnv.sqlQuery("SELECT test(b) FROM table1 ORDER BY c");
//		DataStream<Row> stream2 = tableEnv.toAppendStream(sqlResult2,Row.class,qConfig);
//		stream2.addSink(new RichSinkFunction<Row>() {
//			@Override
//			public void invoke(Row value) throws Exception {
//				writerFile(value.toString());
//			}
//
//			public  void writerFile(String s) throws IOException {
//				Path logFile = Paths.get("./LearnFlink/src/main/resources/test1.txt");
//				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
//					writer.newLine();
//					writer.write(s);
//				}
//			}
//		});

		sEnv.execute();
	}
}
