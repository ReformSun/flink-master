package com.test.learnWindows;

import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;

public class TestHopMain {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		testMethod1(env);
		try {
			env.execute("TestHopMain");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamExecutionEnvironment env) {
		DataStreamSource<String> input = env.addSource(KafkaUtil.getKafkaTableSource("ddddddd"));
		input.flatMap(new FlatMapFunction<String, SunWordWithCount>() {
			@Override
			public void flatMap(String value, Collector<SunWordWithCount> out) throws Exception {
				for (String ss:value.split("\\|{1}")){
					out.collect(
						new SunWordWithCount(ss,1)
					);
				}
			}
		}).keyBy("word").window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))).reduce(new ReduceFunction<SunWordWithCount>() {
			@Override
			public SunWordWithCount reduce(SunWordWithCount value1, SunWordWithCount value2) throws Exception {
				return new SunWordWithCount(value1.word,value1.count + value2.count);
			}
		}).addSink(new CustomWordCountPrint());


	}
}
