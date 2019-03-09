package com.test.sdn;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CustomWindowFunction implements WindowFunction<Tuple8<String,String,String,String,Long,String,Boolean,Long>, String, String, TimeWindow>{
	@Override
	public void apply(String s, TimeWindow window, Iterable<Tuple8<String, String, String, String, Long, String, Boolean, Long>> input, Collector<String> out) throws Exception {
		out.collect("ccccccc");
	}
}
