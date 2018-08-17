package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestMain7 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		CsvTableSource.Builder csvTableSourceBuilder = CsvTableSource
			.builder();


		CsvTableSource csvTableSource = csvTableSourceBuilder.path("/Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/testCsv.csv")//文件路径
			.field("name", Types.STRING)
			.field("age", Types.INT)
			.field("rex", Types.STRING)
			.ignoreFirstLine()//忽略第一行
			.build();

		tableEnv.registerTableSource("testTable", csvTableSource);
		Table sqlResult = tableEnv.sql("select * from testTable");

		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,Row.class,qConfig);
		DataStream<Row> stream2 = tableEnv.toAppendStream(sqlResult,Row.class,qConfig);
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
