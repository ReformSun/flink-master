package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint_Sum;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

import java.util.TimeZone;

public class TestMain18 {
	public static void main(String[] args) {
		final CustomStreamEnvironment sEnv = new CustomStreamEnvironment();
//		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
//		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true,"ccc");
		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource2(60000);
		tableEnv.registerTableSource("filesource", fileTableSource);

//		testMethod1(tableEnv);
//		testMethod2(tableEnv);
		testMethod3(tableEnv);
//		StreamGraph streamGraph = sEnv.getStreamGraph();
//		System.out.println(streamGraph.getStreamingPlanAsJSON());
		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w,test1")
			.select("SUM(user_count) as value1,w.start,test1");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP,Types.STRING);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint_Sum("test.txt"));
	}

	public static void testMethod2(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w,test1,test2")
			.select("SUM(user_count) as value1,w.start,w.end,test1,test2");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP,Types.STRING,Types.STRING);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint_Sum("test.txt"));
	}

	public static void testMethod3(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start as start_time");

//		RowTypeInfo rowTypeInfo1 = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
//		SingleOutputStreamOperator<Row> stream1 = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo1, qConfig);
//		stream1.addSink(new CustomRowPrint("test1.txt"));

		Table sqlResult2 = sqlResult.select("value1 == 0 as value2,start_time");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult2, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test.txt"));
	}
}
