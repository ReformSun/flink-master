package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.sink.CustomCrowSumPrint;
import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint_Sum;
import com.test.sink.InfluxDBSink;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class TestMain16 {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
//		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true);
		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource(300);
		tableEnv.registerTableSource("filesource", fileTableSource);

		testMethod1(tableEnv);
//		testMethod2(tableEnv);
//		testMethod3(tableEnv);

		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 测试事件现象
	 * 当第一次启动有kafka的连接器时。数据没有统计没有问题，这时停止任务执行，再次启动，向kafka中发送和上次相同
	 * 的数据，只能统计一点数据，以后的数据都不会统计
	 * 出现这种问题的原因：上次kafka主题中还有残留数据没有被消费，再启动任务就会消费原来没有消费的数据，这是的数据时间
	 * 大于再次发送的数据时间，再次发送的数据在窗口计算中，会被认为是过期数据，不会再参与统计
	 * @param tableEnv
	 */
	public static void testMethod1(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.sqlQuery("SELECT AVG(user_count) as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM filesource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test.txt"));

		DataStream<CRow> dataStream1 = WindowOutputTag.getDataStream();
		dataStream1.addSink(new CustomCrowSumPrint());
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
		stream.addSink(new CustomRowPrint_Sum("test.txt"));

		DataStream<CRow> dataStream1 = WindowOutputTag.getDataStream();
		dataStream1.addSink(new CustomCrowSumPrint());

	}

	/**
	 * 测试union all和union
	 * @param tableEnv
	 */
	public static void testMethod4(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint_Sum("test.txt"));
	}



}
