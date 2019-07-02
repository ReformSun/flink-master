package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.sink.CustomPrint;
import com.test.sink.CustomPrintTuple3;
import com.test.util.DataUtil;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.OutputTag;

import java.util.List;

public class TestOutPutTag extends AbstractTestMain11{
	public static void main(String[] args) {
		try{
//			testMethod1();
//			testMethod2();
//			testMethod3();
//			testMethod4();
			testMethod5();
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		List<Tuple2<String,Long>> list = getTestMain12data();
		DataStreamSource<Tuple2<String,Long>> dataStreamSource = env.fromCollection(list);
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(Types.STRING(),Types.LONG());
		OutputTag<Tuple2<String,Long>> outputTag = new OutputTag("aa",tupleTypeInfo);
		dataStreamSource.getSideOutput(outputTag).map(new MapFunction<Tuple2<String,Long>, String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("aaaaaaa");
				return "cccc";
			}
		}).print();
		OutputTag<Tuple2<String,Long>> outputTag1 = new OutputTag("cc",tupleTypeInfo);
		dataStreamSource.getSideOutput(outputTag1).map(new MapFunction<Tuple2<String,Long>, String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("cccccc");
				return "cccc";
			}
		}).print();
	}

	public static void testMethod2(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3())
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});

		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));

		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.INT, org.apache.flink.api.common.typeinfo.Types.LONG);
		OutputTag outputTag = new OutputTag("aaa",tupleTypeInfo);

		SingleOutputStreamOperator dataStream = windowedStream.sideOutputLateData(outputTag).trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);
		SingleOutputStreamOperator dataStream2 = dataStream.map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return "aaa";
			}
		});
		dataStream2.addSink(new SinkFunction() {
			@Override
			public void invoke(Object value) throws Exception {
				System.out.println("aa");
			}
		});
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);

		dataStream2.getSideOutput(outputTag).map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return value.toString();
			}
		}).print();

	}

	public static void testMethod3(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3())
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});

		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));

		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.INT, org.apache.flink.api.common.typeinfo.Types.LONG);
		OutputTag outputTag = new OutputTag("aaa",tupleTypeInfo);

		SingleOutputStreamOperator dataStream = windowedStream.sideOutputLateData(outputTag).trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);

		dataStream.getSideOutput(outputTag).map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return value.toString();
			}
		}).print();

	}

	/**
	 * 测试侧边输出在哪里设置比较合适
	 */
	public static void testMethod4(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3(2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});

		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));

		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.INT, org.apache.flink.api.common.typeinfo.Types.LONG);
		OutputTag outputTag = new OutputTag("aaa",tupleTypeInfo);

		SingleOutputStreamOperator dataStream = windowedStream.sideOutputLateData(outputTag).trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);
		SingleOutputStreamOperator dataStream2 = dataStream.map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return "aaa";
			}
		}).map(new MapFunction<String,String>() {
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		});
		dataStream2.addSink(new SinkFunction() {
			@Override
			public void invoke(Object value) throws Exception {
				System.out.println("aa");
			}
		});
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
		// 并不能接受到
		dataStream2.getSideOutput(outputTag).map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return value.toString();
			}
		}).print();
		// 可以接受到过期数据
		dataStream.getSideOutput(outputTag).map(new MapFunction<Tuple3<String,Integer,Long>,String>() {
			@Override
			public String map(Tuple3<String,Integer,Long> value) throws Exception {
				return value.toString();
			}
		}).print();
	}

	public static void testMethod5(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3(2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.getField(0);
				}
			});

		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));

		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.INT, org.apache.flink.api.common.typeinfo.Types.LONG);
		OutputTag outputTag = new OutputTag("aaa",tupleTypeInfo);

		SingleOutputStreamOperator dataStream = windowedStream.sideOutputLateData(outputTag).trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);

		// 可以接受到过期数据
		dataStream.getSideOutput(outputTag).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3(2)).keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.getField(0);
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0))).trigger(new EventTimeTrigger())
			.sum(1).union(dataStream).addSink(new CustomPrintTuple3("过期数据"));
	}
}
