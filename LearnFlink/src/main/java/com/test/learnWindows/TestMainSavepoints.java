package com.test.learnWindows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestMainSavepoints {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");


	}
}
