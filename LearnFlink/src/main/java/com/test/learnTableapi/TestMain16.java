package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.sink.CustomCrowSumPrint;
import com.test.sink.CustomRowPrint;
import com.test.util.FileWriter;
import com.test.util.StreamExecutionEnvUtil;
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
import org.apache.flink.table.api.java.WindowOutputTag;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.util.TimeZone;

public class TestMain16 {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment(null);
		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true);
//		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource(1000);
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
	 * {@link org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan}
	 * {@link org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan}/PeriodicWatermarkAssignerWrapper
	 * {@link org.apache.flink.table.runtime.RowtimeProcessFunction}
	 * {@link org.apache.flink.streaming.api.operators.ProcessOperator}
	 * {@link org.apache.flink.table.runtime.CRowOutputProcessRunner}
	 * {@link org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps}
	 * {@link org.apache.flink.table.plan.nodes.datastream.CustomDataStreamGroupWindowAggregate}
	 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}
	 *
	 * 两分钟内乱序数据 不增加传入时间间隔 数据统计完整 结果数据未出现乱序情况
	 * 两分钟内乱序数据 增减传入时间间隔 1秒钟
	 *
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
		stream.addSink(new CustomRowPrint("test.txt"));

		DataStream<CRow> dataStream1 = WindowOutputTag.getDataStream();
		dataStream1.addSink(new CustomCrowSumPrint());

	}


}
