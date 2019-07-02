package com.test.alarm;

import com.test.customAssignTAndW.CustomAssignerTimesTampTuple_pr;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3_pr;
import com.test.filesource.FileSourceTuple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.operator.CustomTimestampsAndPeriodicWatermarksOperator;
import com.test.sink.CustomPrintTuple;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestAlarm1 {
	static StreamExecutionEnvironment env = null;

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
//		env = new CustomStreamEnvironment(configuration);
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.setAutoWatermarkInterval(30000);
		executionConfig.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		try{
//			testMethod1();
			testMethod2();
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
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(70000))
			.setParallelism(1);
		dataStreamSource1.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple_pr<>(2))
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.f0;
				}
			}).window(TumblingEventTimeWindows.of(Time.seconds(60)))
			.sum(1)
			.addSink(new CustomPrintTuple<>("test.txt"));
	}

	public static void testMethod2(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(70000));

		CustomTimestampsAndPeriodicWatermarksOperator customTimestampsAndPeriodicWatermarksOperator = new CustomTimestampsAndPeriodicWatermarksOperator(new
			CustomAssignerTimesTampTuple_pr<>(2),()->{
			return new Tuple3<String,Integer,Long>("a",1,System.currentTimeMillis());
		});
		TypeInformation tTypeInformation = dataStreamSource1.getTransformation().getOutputType();

		dataStreamSource1.transform("测试算子",tTypeInformation,customTimestampsAndPeriodicWatermarksOperator)
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.f0;
				}
			}).window(TumblingEventTimeWindows.of(Time.seconds(60)))
			.sum(1)
			.addSink(new CustomPrintTuple<>("test.txt"));
	}
}
