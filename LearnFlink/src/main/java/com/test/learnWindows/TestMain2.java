package com.test.learnWindows;

import com.test.sink.CustomPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class TestMain2 {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
			env.getConfig().disableSysoutLogging();
			env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
			env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
			input = env.addSource(KafkaUtil.getKafkaTableSource("ddddddd"));
//			testMethod1(input);
			testMethod2(input);
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	public static void testMethod1(DataStreamSource<String> input) {
		DataStream<String> dataStream = input.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		});

		DataStream<String> dataStream1 =dataStream.keyBy("a").flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
			}
		});
		dataStream1.addSink(new CustomPrint("test1.txt"));

		DataStream<String> dataStream2 =dataStream.keyBy("b").flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
			}
		});
		dataStream2.addSink(new CustomPrint("test2.txt"));

		DataStream<String> dataStream3 =dataStream.keyBy("c").flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
			}
		});
		dataStream3.addSink(new CustomPrint("test3.txt"));
	}

	public static void testMethod2(DataStreamSource<String> input) {
		DataStream<Map<String,String>> dataStream = input.map(new MapFunction<String, Map<String,String>>() {
			@Override
			public Map<String,String> map(String value) throws Exception {
				return new HashMap<String,String>(){{
					put(value,value);
				}};
			}
		});

		DataStream<String> dataStream1 =dataStream.keyBy("a").flatMap(new FlatMapFunction<Map<String,String>, String>() {
			@Override
			public void flatMap(Map<String,String> value, Collector<String> out) throws Exception {
				out.collect(value.get("a"));
			}
		});
		dataStream1.addSink(new CustomPrint("test1.txt"));
	}
}
