package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTuple;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3_pr;
import com.test.filesource.FileSourceTuple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomPrintTuple;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomRowPrint;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class TestWindow3{
	final static CustomStreamEnvironment env;
	final static StreamTableEnvironment tableEnv;
	static {
		env = new CustomStreamEnvironment();
		tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	}
	public static void main(String[] args) {
		try{
//			testMethod1();
//			testMethod2();
//			testMethod3();
//			testMethod4();
			testMethod5();
//			StreamGraph streamGraph = env.getStreamGraph();
//			System.out.println(streamGraph.getStreamingPlanAsJSON());
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200)).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3_pr<String,Integer,Long>(0,2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.getField(0);
				}
			});
		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)))
			.trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

	public static void testMethod2(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200));
		dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.f0;
			}
		}).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2)).setParallelism(4)
			.keyBy(new NullByteKeySelector()).window(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1).setParallelism(4)
			.addSink(new CustomPrintTuple("test.txt"));
	}

	public static void testMethod3(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200));
		dataStreamSource1.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2))
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.f0;
				}
			}).window(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1).setParallelism(4)
			.addSink(new CustomPrintTuple("test.txt"));
	}

	public static void testMethod4(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200));
		SplitStream<Tuple3<String, Integer, Long>> splitStream = dataStreamSource1.split(new OutputSelector<Tuple3<String, Integer, Long>>() {
			@Override
			public Iterable<String> select(Tuple3<String, Integer, Long> value) {
				if (value.f0.equals("a")){
					return Arrays.asList("a");
				}else if (value.f0.equals("b")){
					return Arrays.asList("b");
				}else if (value.f0.equals("c")){
					return Arrays.asList("c");
				}else if (value.f0.equals("d")){
					return Arrays.asList("d");
				}else {
					return Arrays.asList("other");
				}
			}
		});


		DataStream<Tuple3<String, Integer, Long>> dataStream1 = splitStream.select("a").assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2))
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1);
		DataStream<Tuple3<String, Integer, Long>> dataStream2 = splitStream.select("b").assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2))
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1);
		DataStream<Tuple3<String, Integer, Long>> dataStream3 = splitStream.select("c").assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2))
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1);
		DataStream<Tuple3<String, Integer, Long>> dataStream4 = splitStream.select("d").assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(2))
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(60))).sum(1);


		dataStream1.union(dataStream2).union(dataStream3).union(dataStream4).addSink(new CustomPrintTuple("test.txt"));

	}

	public static void testMethod5(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200));
		SplitStream<Tuple3<String, Integer, Long>> splitStream = dataStreamSource1.split(new OutputSelector<Tuple3<String, Integer, Long>>() {
			@Override
			public Iterable<String> select(Tuple3<String, Integer, Long> value) {
				if (value.f0.equals("a")){
					return Arrays.asList("a");
				}else if (value.f0.equals("b")){
					return Arrays.asList("b");
				}else if (value.f0.equals("c")){
					return Arrays.asList("c");
				}else if (value.f0.equals("d")){
					return Arrays.asList("d");
				}else {
					return Arrays.asList("other");
				}
			}
		});

		DataStream<Tuple3<String, Integer, Long>> dataStream1 = splitStream.select("a");
		DataStream<Tuple3<String, Integer, Long>> dataStream2 = splitStream.select("b");
		DataStream<Tuple3<String, Integer, Long>> dataStream3 = splitStream.select("c");
		DataStream<Tuple3<String, Integer, Long>> dataStream4 = splitStream.select("d");

		tableEnv.registerDataStream("tableA",dataStream1,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableB",dataStream2,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableC",dataStream3,"a,b,c.rowtime");
		tableEnv.registerDataStream("tableD",dataStream4,"a,b,c.rowtime");


		Table table1 = tableEnv.sqlQuery("SELECT SUM(b) as value1,TUMBLE_START(rtime, INTERVAL '60' SECOND) as start_time FROM tableA GROUP BY TUMBLE(rtime, INTERVAL '60' " +
			"SECOND)");
		Table table2 = tableEnv.sqlQuery("SELECT SUM(b) as value1,TUMBLE_START(rtime, INTERVAL '60' SECOND) as start_time FROM tableB GROUP BY TUMBLE(rtime, INTERVAL '60' " +
			"SECOND)");
		Table table3 = tableEnv.sqlQuery("SELECT SUM(b) as value1,TUMBLE_START(rtime, INTERVAL '60' SECOND) as start_time FROM tableC GROUP BY TUMBLE(rtime, INTERVAL '60' " +
			"SECOND)");
		Table table4 = tableEnv.sqlQuery("SELECT SUM(b) as value1,TUMBLE_START(rtime, INTERVAL '60' SECOND) as start_time FROM tableD GROUP BY TUMBLE(rtime, INTERVAL '60' " +
			"SECOND)");

		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP);

		tableEnv.toAppendStream(table1,rowTypeInfo)
			.union(tableEnv.toAppendStream(table2,rowTypeInfo))
			.union(tableEnv.toAppendStream(table3,rowTypeInfo))
			.union(tableEnv.toAppendStream(table4,rowTypeInfo)).addSink(new CustomRowPrint("test.txt"));


	}
}
