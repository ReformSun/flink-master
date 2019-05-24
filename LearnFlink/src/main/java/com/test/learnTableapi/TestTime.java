package com.test.learnTableapi;

import com.test.customAssignTAndW.CustomAssignerWithPeriodicWatermarks;
import com.test.filesource.FileSourceBase;
import com.test.filesource.FileTableSource;
import com.test.sink.CustomCrowSumPrint;
import com.test.sink.CustomRowPrint;
import com.test.util.StreamExecutionEnvUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.WindowOutputTag;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.TimeZone;

public class TestTime {
	public static void main(String[] args) {
		test1();
//		test2();
	}

	/**
	 * 这种不会出现时间问题的原因
	 * 当使用table时。
	 * sql解析生成执行flinkjob任务时在tableSource
	 * {@link org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan}的translateToPlan
	 * 方法调用{@link org.apache.flink.table.plan.nodes.datastream.StreamScan}的convertToInternalRow方法
	 * convertToInternalRow方法会调用自己的generateConversionProcessFunction
	 * 这个方法会通过传入的信息合成一个类 名字是DataStreamSourceConversion$数字
	 * 这个类在processElement方法中调用了{@link org.apache.calcite.runtime.SqlFunctions}的SqlFunctions.toLong((java.sql.Timestamp) in1.getField(2))
	 * 方法，这个方法会把穿进去的时间字段转成long类型，并且使用TimeZone.getDefault();进行了时区转换
	 * 为什么最后传出的时间字段没有问题 窗口数据统计好之后会执行下面这个类的
	 * {@link org.apache.flink.table.runtime.aggregate.TimeWindowPropertyCollector }collect方法
	 * 这个类会调用{@link org.apache.calcite.runtime.SqlFunctions}SqlFunctions.internalToTimestamp(windowStart)
	 * 进行了二次转换。有转换回来了
	 *
	 * 测试可以查看{@link com.test.TestSqlFunctions}
	 *
	 *
	 */
	public static void test1(){
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
		TableConfig tableConfig = new TableConfig();
//		tableConfig.setIsEnableWindowOutputTag(true);
//		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileTableSource fileTableSource = FileUtil.getFileTableSource(300);
		tableEnv.registerTableSource("filesource", fileTableSource);

		testMethod1(tableEnv);

		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 这种方式处出现时间相差8个小时的情况
	 *
	 * {@link org.apache.flink.table.plan.nodes.datastream.DataStreamCalc} 116行
	 *
	 * {@link org.apache.flink.table.runtime.aggregate.TimeWindowPropertyCollector }
	 * SqlFunctions.internalToTimestamp(windowStart)
	 * 出现相差8小时的问题
	 * 只经过了{@link org.apache.flink.table.runtime.aggregate.TimeWindowPropertyCollector }的
	 * 一次转换，所以输出数据向后退了8个小时
	 * 没有{@link org.apache.flink.table.plan.nodes.datastream.StreamScan}聚合类的前一次转换，因为
	 * 不是从tablesource开始的
	 * 测试查看{@link com.test.TestSqlFunctions}
	 *
	 */
	public static void test2(){
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
		TableConfig tableConfig = new TableConfig();
		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FileSourceBase<Row> fileSourceBase = FileUtil.getFileSourceBase();
		SingleOutputStreamOperator<Row> singleOutputStreamOperator = sEnv.addSource(fileSourceBase,"fileS",fileSourceBase.getProducedType()).assignTimestampsAndWatermarks(new
			CustomAssignerWithPeriodicWatermarks(2));
		SplitStream<Row> splitStream = singleOutputStreamOperator.split(new OutputSelector<Row>() {
			@Override
			public Iterable<String> select(Row value) {
				String name = (String) value.getField(0);
				if (name.equals("小张")){
					return Arrays.asList("小张");
				}else {
					return Arrays.asList("other");
				}
			}
		});
		String field = "user_name,user_count,_sysTime.rowtime";
		DataStream<Row> dataStream = splitStream.select("小张");
		tableEnv.registerDataStream("filesource",dataStream,field);
		testMethod1(tableEnv);
		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 自己输入的时间和输出的时间相差8个小时
	 * @param tableEnv
	 */
	public static void testMethod1(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.sqlQuery("SELECT AVG(user_count) as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM filesource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test.txt"));

	}
}
