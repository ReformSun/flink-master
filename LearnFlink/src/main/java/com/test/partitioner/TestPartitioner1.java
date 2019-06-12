package com.test.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;

public class TestPartitioner1 {
	public static void main(String[] args) {
		testMethod2();
	}
	public static void testMethod1(){
		KeySelector<Tuple3<String,Integer,Long>,String> keySelector = new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.getField(0);
			}
		};
		KeyGroupStreamPartitioner keyGroupStreamPartitioner = new KeyGroupStreamPartitioner(keySelector,3);

	}

	public static void testMethod2(){
	    String key = "aa";
	    key = "bbkdkdkkkkkkeeeeeeee";
	    int maxParallelism = 3;
	    int numberOfOutputChannels = 4;
		System.out.println(KeyGroupRangeAssignment.assignToKeyGroup(key,maxParallelism));
		System.out.println(KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfOutputChannels));
	}

	public static void testMethod3(){

	}
}
