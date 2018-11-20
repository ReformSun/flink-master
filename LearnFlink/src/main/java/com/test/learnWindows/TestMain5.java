package com.test.learnWindows;

import com.test.sink.CustomPrint;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import socket.SocketWindowWordCount;
import test.SunWordWithCount;

public class TestMain5 {
	public static void main(String[] args) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
			env.getConfig().disableSysoutLogging();
			env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
			env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
			input = env.addSource(KafkaUtil.getKafkaTableSource("ddddddd")).setParallelism(1);
			testMethod1(input);
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
		input.flatMap(new FlatMapFunction<String, SunWordWithCount>() {
			@Override
			public void flatMap(String value, Collector<SunWordWithCount> out) throws Exception {
				out.collect(new SunWordWithCount(value,1));
			}
		}).setParallelism(1).keyBy("word").reduce(new RichReduceFunction<SunWordWithCount>() {
			@Override
			public SunWordWithCount reduce(SunWordWithCount value1, SunWordWithCount value2) throws Exception {
				return new SunWordWithCount(value1.word,value1.count + value2.count);
			}
		}).addSink(new CustomWordCountPrint()).setParallelism(2);
	}
}
