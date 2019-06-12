package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.sink.CustomPrintTuple;
import com.test.util.DataUtil;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class TestJoin1 {
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
		dataStreamSource1.join(dataStreamSource2).where(new KeySelector<Tuple3<Long,Integer,String>, String>() {
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
			.apply(new JoinFunction<Tuple3<Long,Integer,String>, Tuple3<Long,Integer,String>, Tuple3<Long,Integer,String>>() {
				private int a;
				@Override
				public Tuple3<Long, Integer, String> join(Tuple3<Long, Integer, String> first, Tuple3<Long, Integer, String> second) throws Exception {
					System.out.println("start:" + a);
					System.out.println(first.toString());
					System.out.println(second.toString());
					System.out.println("end:"+ a);
					a++;
					return new Tuple3<>(first.f0,first.f1 + second.f1,first.f2);
				}
			}).addSink(new CustomPrintTuple<>("test.txt"));
	}
}
