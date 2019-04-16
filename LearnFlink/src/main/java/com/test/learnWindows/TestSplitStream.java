package com.test.learnWindows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSplitStream extends AbstractTestMain11{
	public static void main(String[] args) {
		try{
			testMethod1();
//			testMethod2();
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
		List<Tuple2<String,Long>> list = getTestSplitStream(1,1,3);
		DataStreamSource<Tuple2<String,Long>> dataStreamSource = env.fromCollection(list);
		SplitStream splitStream = dataStreamSource.split(new OutputSelector<Tuple2<String, Long>>() {
			@Override
			public Iterable<String> select(Tuple2<String, Long> value) {
				if (value.f0.equals("a")){
					return Arrays.asList("aa");
				}else if (value.f0.equals("b")){
					return Arrays.asList("bb","cc");
				}else {
					return Arrays.asList("cc");
				}
			}
		});
		splitStream.select("aa").map(new MapFunction<Tuple2<String, Long>,String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("aaa");
				return "aa";
			}
		}).print();
		splitStream.select("bb").map(new MapFunction<Tuple2<String, Long>,String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("bbb");
				return "aa";
			}
		}).print();
		splitStream.select("cc").map(new MapFunction<Tuple2<String, Long>,String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("ccc");
				return "aa";
			}
		}).print();
	}
}
