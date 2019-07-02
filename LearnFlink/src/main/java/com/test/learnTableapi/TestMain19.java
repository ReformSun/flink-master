package com.test.learnTableapi;

import com.test.customAssignTAndW.CustomAssignerWithPeriodicWatermarks;
import com.test.filesource.FileSource;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomRowPrint;
import com.test.util.URLUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class TestMain19 {
	final static CustomStreamEnvironment env;
	final static StreamTableEnvironment tableEnv;
	static {
		env = new CustomStreamEnvironment();
		tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	}

	public static void main(String[] args) {
		try{
			testMethod1();
//			testMethod2();
//			testMethod3();
//			testMethod4();
//			StreamGraph streamGraph = env.getStreamGraph();
//			System.out.println(streamGraph.getStreamingPlanAsJSON());
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow19");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();
		tableSchemaBuilder.field("user_count", Types.INT())
			.field("user_name",Types.STRING())
			.field("_sysTime",Types.LONG());
		JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema(tableSchemaBuilder.build().toRowType());
		FileSource<Row> fileSource = new FileSource<Row>(URLUtil.baseUrl + "dataTestTableFile.txt",jsonRowDeserializationSchema);
		DataStreamSource<Row> dataStreamSource = env.addSource(fileSource,tableSchemaBuilder.build().toRowType());
		SplitStream<Row> splitStream = dataStreamSource.split(new OutputSelector<Row>(){
			@Override
			public Iterable<String> select(Row value) {
				String name = (String) value.getField(1);
				if (name.equals("a")){
					return Arrays.asList("a");
				}else if (name.equals("b")){
					return Arrays.asList("b");
				}else if (name.equals("c")){
					return Arrays.asList("c");
				}else if (name.equals("d")){
					return Arrays.asList("d");
				}else {
					return Arrays.asList("other");
				}
			}
		});

		DataStream<Row> dataStream1 = splitStream.select("a").assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks(2));
		DataStream<Row> dataStream2 = splitStream.select("b").assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks(2));
		DataStream<Row> dataStream3 = splitStream.select("c").assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks(2));
		DataStream<Row> dataStream4 = splitStream.select("d").assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks(2));

		tableEnv.registerDataStream("tableA",dataStream1,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableB",dataStream2,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableC",dataStream3,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableD",dataStream4,"a,b,c.rowtime");


		Table table1 = tableEnv.sqlQuery("SELECT SUM(a) as value1,TUMBLE_START(c, INTERVAL '60' SECOND) as start_time FROM tableA GROUP BY TUMBLE(c, INTERVAL '60' " +
			"SECOND)");
		Table table2 = tableEnv.sqlQuery("SELECT SUM(a) as value1,TUMBLE_START(c, INTERVAL '60' SECOND) as start_time FROM tableB GROUP BY TUMBLE(c, INTERVAL '60' " +
			"SECOND)");
		Table table3 = tableEnv.sqlQuery("SELECT SUM(a) as value1,TUMBLE_START(c, INTERVAL '60' SECOND) as start_time FROM tableC GROUP BY TUMBLE(c, INTERVAL '60' " +
			"SECOND)");
		Table table4 = tableEnv.sqlQuery("SELECT SUM(a) as value1,TUMBLE_START(c, INTERVAL '60' SECOND) as start_time FROM tableD GROUP BY TUMBLE(c, INTERVAL '60' " +
			"SECOND)");

		RowTypeInfo rowTypeInfo = new RowTypeInfo(org.apache.flink.api.common.typeinfo.Types.INT, org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP);

		tableEnv.toAppendStream(table1,rowTypeInfo)
			.union(tableEnv.toAppendStream(table2,rowTypeInfo))
			.union(tableEnv.toAppendStream(table3,rowTypeInfo))
			.union(tableEnv.toAppendStream(table4,rowTypeInfo)).addSink(new CustomRowPrint("test.txt"));
	}
}
