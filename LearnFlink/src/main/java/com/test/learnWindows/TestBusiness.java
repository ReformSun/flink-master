package com.test.learnWindows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TestBusiness extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
//			testMethod1();
			testMethod2();
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		DataStreamSource<String> dataSource = getInput("testto");
		dataSource.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {

				System.out.println(value);

				return "ddd";
			}
		}).print();
	}


	public static void testMethod2(){
		DataStreamSource<String> dataSource = getInput("testto");
		OutputTag<String> outputTag = new OutputTag("aa", Types.STRING());
		DataStream<String> dataStream = dataSource.getSideOutput(outputTag);
		OutputTag<String> outputTag1 = new OutputTag("ccc",Types.STRING());
		DataStream<String> dataStream1 = dataSource.getSideOutput(outputTag1);
		dataStream.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				System.out.println("aa:" + value);
				return "ccccccc";
			}
		}).print();
		dataStream1.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				System.out.println("ccc:" + value);
				return "ddddddd";
			}
		}).print();
	}
}
