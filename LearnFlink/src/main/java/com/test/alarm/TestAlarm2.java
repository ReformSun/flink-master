package com.test.alarm;

import com.test.customAssignTAndW.CustomAssignerTimesTampTuple_pr;
import com.test.customAssignTAndW.CustomAssignerWithPeriodicWatermarks;
import com.test.filesource.FileTableSource;
import com.test.learnState.CustomStreamEnvironment;
import com.test.learnTableapi.FileUtil;
import com.test.operator.CustomTimestampsAndPeriodicWatermarksOperator;
import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint_Sum;
import com.test.source.CustomTableSource;
import com.test.util.URLUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.util.TimeZone;

public class TestAlarm2 {
	public static void main(String[] args) {
		final CustomStreamEnvironment sEnv = new CustomStreamEnvironment();
//		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true,"ccc");
		tableConfig.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		CustomTableSource.Builder builder = CustomTableSource.builder();

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder
			.field("user_name", Types.STRING)
			.field("user_count",Types.LONG)
			.field("_sysTime", Types.SQL_TIMESTAMP)
			.build();

		     CustomTableSource customTableSource = builder.setSchema(tableSchema)
				 .setRowTime("_sysTime")
				 .setInterval(120000)
				 .setWatermarkStrategy(null)
				 .build();
		tableEnv.registerTableSource("filesource", customTableSource);

//		testMethod1(tableEnv);
		testMethod2(tableEnv);
//		StreamGraph streamGraph = sEnv.getStreamGraph();
//		System.out.println(streamGraph.getStreamingPlanAsJSON());
		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod2(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'");
		TypeInformation[] typeInformations = new TypeInformation[sqlResult.getSchema().getTypes().length];
		String fields = null;
		int time_index = 0;
		StringBuilder stringBuilder = new StringBuilder();
		for (int j = 0; j < sqlResult.getSchema().getTypes().length; j++) {
			TypeInformation typeInformation = sqlResult.getSchema().getType(j).get();
			String fieldName = sqlResult.getSchema().getColumnName(j).get();
			if (typeInformation instanceof TimeIndicatorTypeInfo){
				typeInformation = Types.SQL_TIMESTAMP;
				time_index = j;
				stringBuilder.append(fieldName).append(".rowtime,");
			}else {
				stringBuilder.append(fieldName).append(",");
			}
			typeInformations[j]  = typeInformation;
		}

		fields = stringBuilder.substring(0,stringBuilder.length() - 1);
		RowTypeInfo rowTypeInfo1 = new RowTypeInfo(typeInformations);
		DataStream<Row> stream1 = tableEnv.toAppendStream(sqlResult,rowTypeInfo1, qConfig);
		CustomAssignerWithPeriodicWatermarks customAssignerWithPeriodicWatermarks = new CustomAssignerWithPeriodicWatermarks(time_index);
		CustomTimestampsAndPeriodicWatermarksOperator customTimestampsAndPeriodicWatermarksOperator = new CustomTimestampsAndPeriodicWatermarksOperator
			(customAssignerWithPeriodicWatermarks,new AssignerRow(typeInformations));
		SingleOutputStreamOperator<Row> singleOutputStreamOperator = stream1.transform("",stream1.getType(),customTimestampsAndPeriodicWatermarksOperator);
		tableEnv.registerDataStream("tableName1",singleOutputStreamOperator,fields);

		Table table1 = tableEnv.scan("tableName1").window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start as start_time");
		Table table2 = table1.select("value1 == 1 as value2,start_time");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP);
		DataStream<Row> stream2 = tableEnv.toAppendStream(table2,rowTypeInfo,qConfig);
		stream2.addSink(new CustomRowPrint("test.txt"));
	}

	public static void testMethod1(StreamTableEnvironment tableEnv){
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnv.scan("filesource")
			.where("user_name = '小张'");


		TypeInformation[] typeInformations = new TypeInformation[sqlResult.getSchema().getTypes().length];
		String fields = null;
		int time_index = 0;
		StringBuilder stringBuilder = new StringBuilder();
		for (int j = 0; j < sqlResult.getSchema().getTypes().length; j++) {
			TypeInformation typeInformation = sqlResult.getSchema().getType(j).get();
			String fieldName = sqlResult.getSchema().getColumnName(j).get();
			if (typeInformation instanceof TimeIndicatorTypeInfo){
				typeInformation = Types.SQL_TIMESTAMP;
				time_index = j;
				stringBuilder.append(fieldName).append(".rowtime,");
			}else {
				stringBuilder.append(fieldName).append(",");
			}
			typeInformations[j]  = typeInformation;
		}

		fields = stringBuilder.substring(0,stringBuilder.length() - 1);



//		AssignerRow assignerRow = new AssignerRow(typeInformations);
//		System.out.println(assignerRow.getElement());

		RowTypeInfo rowTypeInfo1 = new RowTypeInfo(typeInformations);
		DataStream<Row> stream1 = tableEnv.toAppendStream(sqlResult,rowTypeInfo1, qConfig);
		CustomAssignerWithPeriodicWatermarks customAssignerWithPeriodicWatermarks = new CustomAssignerWithPeriodicWatermarks(time_index);
		CustomTimestampsAndPeriodicWatermarksOperator customTimestampsAndPeriodicWatermarksOperator = new CustomTimestampsAndPeriodicWatermarksOperator
			(customAssignerWithPeriodicWatermarks,new AssignerRow(typeInformations));
		SingleOutputStreamOperator<Row> singleOutputStreamOperator = stream1.transform("",stream1.getType(),customTimestampsAndPeriodicWatermarksOperator);
		tableEnv.registerDataStream("=o",singleOutputStreamOperator,fields);
		Table table1 = tableEnv.scan("=o").window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start as start_time");
		Table table2 = table1.select("value1 == 1 as value2,start_time");
		Table table = sqlResult.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start as start_time");
		Table sqlResult2 = table.select("value1 > 0 as value2,start_time");

		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP);
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult2, rowTypeInfo, qConfig);
		DataStream<Row> stream2 = tableEnv.toAppendStream(table2,rowTypeInfo,qConfig);
		tableEnv.registerDataStream("tablec",stream.union(stream2));
		Table table3 = tableEnv.scan("tablec");

		DataStream<Row> stream3 = tableEnv.toAppendStream(table3,rowTypeInfo,qConfig);
		stream3.addSink(new CustomRowPrint("test.txt"));

	}
}
