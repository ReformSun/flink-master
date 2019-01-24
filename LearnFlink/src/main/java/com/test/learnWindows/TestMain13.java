package com.test.learnWindows;

import com.test.sink.CustomPrint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestMain13 extends AbstractTestMain1{
	public static void main(String[] args) {
		testMethod1();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1() {
		DataStream<String> dataStream = getInput("assdddd");
		dataStream.addSink(new CustomPrint("test.txt"));
	}
}
