package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomPrintTuple;
import com.test.util.DataUtil;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public class TestCoGrouped1 {
	static final CustomStreamEnvironment env = new CustomStreamEnvironment();
	public static void main(String[] args) {
		testMethod1();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void testMethod1(){
		List<Tuple3<Long,Integer,String>> list1 = DataUtil.getTuple3_Int(1);
		List<Tuple3<Long,Integer,String>> list2 = DataUtil.getTuple3_Int(2);
		DataStream<Tuple3<Long,Integer,String>> dataStreamSource1 = env.fromCollection(list1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(0));
		DataStream<Tuple3<Long,Integer,String>> dataStreamSource2 = env.fromCollection(list2)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(0));
		dataStreamSource1.coGroup(dataStreamSource2).where(new KeySelector<Tuple3<Long,Integer,String>, String>() {
			@Override
			public String getKey(Tuple3<Long, Integer, String> value) throws Exception {
				return value.f2;
			}
		}).equalTo(new KeySelector<Tuple3<Long, Integer, String>, String>() {
			@Override
			public String getKey(Tuple3<Long, Integer, String> value) throws Exception {
				return value.f2;
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)))
			.apply(new CoGroupFunction<Tuple3<Long,Integer,String>, Tuple3<Long,Integer,String>, Tuple3<Long,Integer,String>>() {
				private int a;
				@Override
				public void coGroup(Iterable<Tuple3<Long, Integer, String>> first, Iterable<Tuple3<Long, Integer, String>> second, Collector<Tuple3<Long, Integer, String>> out) throws Exception {
					System.out.println("start:" + a);
					Iterator<Tuple3<Long, Integer, String>> iterator = first.iterator();
					while (iterator.hasNext()){
						Tuple3<Long, Integer, String> tuple3 = iterator.next();
						System.out.println(tuple3.toString());
					}
					Iterator<Tuple3<Long, Integer, String>> iterator1 = second.iterator();
					while (iterator1.hasNext()){
						Tuple3<Long, Integer, String> tuple3 = iterator1.next();
						System.out.println(tuple3.toString());
					}
					System.out.println("end:" + a);
					a++;
					out.collect(new Tuple3<>(11L,1,"cc"));
				}
			}).addSink(new CustomPrintTuple<>("test.txt"));
	}
}
