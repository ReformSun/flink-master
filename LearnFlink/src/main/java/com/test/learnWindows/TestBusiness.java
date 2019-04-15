package com.test.learnWindows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class TestBusiness extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
			testMethod1();
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
}
