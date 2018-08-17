package com.test.learnTableapi;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class TestMain8 {
	public static void main(String[] args) throws Exception {
		testMethod1();
	}

	public static void testMethod1() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		StreamQueryConfig qConfig = tableEnv.queryConfig();

//		RowTypeInfo

		Row row = new Row(3);
		row.setField(0,"ddd");
		row.setField(1,"ccc");
		row.setField(2,"bbb");

		DataStream<Row> stream1 = sEnv.fromElements(row);
		Table table = tableEnv.fromDataStream(stream1,"count, word,cc");

		tableEnv.registerTable("tabledd",table);

		String explanation = tableEnv.explain(table);
		System.out.println(explanation);
		//查询
		Table sqlResult = tableEnv.sqlQuery("select * FROM tabledd");

		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,Row.class,qConfig);
		Table table2 = tableEnv.fromDataStream(stream,"count, word,cc");
		//将数据写出去
//        sqlResult.printSchema();
		sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
		//执行
		sEnv.execute();

	}



}
