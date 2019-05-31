package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.sink.CustomCrowSumPrint;
import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint_Sum;
import com.test.sink.InfluxDBSink;
import com.test.util.StreamExecutionEnvUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.api.java.WindowOutputTag;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class TestInfluxDB {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
//		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true);
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
	 * 测试设置不为1时 向时序数据库内插值
	 * @param tableEnv
	 */
	public static void testMethod1(StreamTableEnvironment tableEnv){
		Properties properties = new Properties();
		// 设置influxdb 配置参数
		properties.put("influxDBUrl", "172.31.24.36:8086");
		properties.put("username", "testInflux");
		properties.put("password", "123456");
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start as timee");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint_Sum("test.txt"));
		Map<Integer,String> tagMap = new HashMap<>();
		Map<Integer,String> fieldMap = new HashMap<>();
		fieldMap.put(0,"value1");
		tagMap.put(1,"timee");
		stream.addSink(InfluxDBSink.builder().withInfluxDBProperties(properties)
			.setDatabase("testDB")
			.setMeasurement("test123456") // 表名
			.setTagMap(tagMap)
			.setFieldMap(fieldMap)
			.setTime_index(1)
			.setFlushOnCheckpoint(true).build()).setParallelism(8);

		DataStream<CRow> dataStream1 = WindowOutputTag.getDataStream();
		dataStream1.addSink(new CustomCrowSumPrint());
	}
}
