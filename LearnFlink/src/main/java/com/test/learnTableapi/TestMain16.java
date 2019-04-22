package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

public class TestMain16 {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource();
		tableEnv.registerTableSource("filesource", fileTableSource);

		testMethod1(tableEnv);
//		testMethod2(tableEnv);

		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.sqlQuery("SELECT AVG(user_count) as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM filesource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test.txt"));
	}

	public static void testMethod2(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("AVG(user_count) as value1,w.start");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test.txt"));
	}


}
