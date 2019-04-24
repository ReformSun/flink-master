package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.util.TimeZone;

public class TestMain16 {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
//		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource();
		tableEnv.registerTableSource("filesource", fileTableSource);

//		testMethod1(tableEnv);
		testMethod2(tableEnv);

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

	/**
	 * {@link org.apache.flink.table.runtime.CRowOutputProcessRunner}
	 * {@link org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps}
	 * @param tableEnv
	 */
	public static void testMethod2(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
//		DataStream<Row> dataStream = stream.getSideOutput(new OutputTag("aaa",rowTypeInfo));
//		dataStream.addSink(new SinkFunction<Row>() {
//			@Override
//			public void invoke(Row value) throws Exception {
//				System.out.println(value.toString());
//			}
//		});
//		DataStream<Row> dataStream2 = stream.getSideOutput(new OutputTag("ccc",rowTypeInfo));
		stream.addSink(new CustomRowPrint("test.txt"));
	}


}
