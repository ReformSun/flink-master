package com.test.learnWindows;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class AbstractTestMain1 {
	public static final StreamExecutionEnvironment env;
	public static DataStreamSource<String> input;
	public static DataStreamSource<String> input2;
	static {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		input =  env.addSource(KafkaUtil.getKafkaConsumer09Source("ddddddd")).setParallelism(1);
		input2 = env.addSource(KafkaUtil.getKafkaConsumer09Source("ccccccc")).setParallelism(1);
	}



}
