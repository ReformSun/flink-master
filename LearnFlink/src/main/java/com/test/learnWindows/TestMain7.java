package com.test.learnWindows;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 学习flink的jion操作
 */
public class TestMain7 {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			input = env.addSource(KafkaUtil.getKafkaConsumer09Source("ddddddd")).setParallelism(1);
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

	}
}
