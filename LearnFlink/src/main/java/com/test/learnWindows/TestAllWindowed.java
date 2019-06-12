package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3_pr;
import com.test.filesource.FileSourceTuple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomPrintTuple3;
import com.test.util.DataUtil;
import com.test.util.TimeUtil;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public class TestAllWindowed {
	static final CustomStreamEnvironment env = new CustomStreamEnvironment();
	public static void main(String[] args) {
//		testMethod1();
//		testMethod2();
		testMethod3();
		try {
			env.execute();
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
		List<Tuple3<Long,Integer,String>> list1 = DataUtil.getTuple3_Int(1);
		DataStream<Tuple3<Long,Integer,String>> dataStreamSource1 = env.fromCollection(list1);
		dataStreamSource1.countWindowAll(5).apply(new AllWindowFunction<Tuple3<Long,Integer,String>, Integer, GlobalWindow>() {
			@Override
			public void apply(GlobalWindow window, Iterable<Tuple3<Long,Integer,String>> values, Collector<Integer> out) throws Exception {
				Iterator<Tuple3<Long,Integer,String>> iterator = values.iterator();
				int sum = 0;
				while (iterator.hasNext()){
					Tuple3<Long,Integer,String> tuple3 = iterator.next();
					sum+=tuple3.f1;
				}
				out.collect(sum);
			}
		}).addSink(new SinkFunction<Integer>() {
			@Override
			public void invoke(Integer value) throws Exception {
				System.out.println("value:"+value);
			}
		});
	}

	public static void testMethod3(){
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		List<Tuple3<Long,Integer,String>> list1 = DataUtil.getTuple3_Int(1);
		DataStream<Tuple3<Long,Integer,String>> dataStreamSource1 = env.fromCollection(list1);
		dataStreamSource1.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(0)).timeWindowAll(Time.seconds(60)).apply(new AllWindowFunction<Tuple3<Long,Integer,
			String>, Integer, TimeWindow>() {
			@Override
			public void apply(TimeWindow window, Iterable<Tuple3<Long, Integer, String>> values, Collector<Integer> out) throws Exception {
				Iterator<Tuple3<Long,Integer,String>> iterator = values.iterator();
				int sum = 0;
				while (iterator.hasNext()){
					Tuple3<Long,Integer,String> tuple3 = iterator.next();
					sum+=tuple3.f1;
				}
				out.collect(sum);
			}
		}).addSink(new SinkFunction<Integer>() {
			@Override
			public void invoke(Integer value) throws Exception {
				System.out.println("value:"+value);
			}
		});
	}
}
