package com.test.learnWindows;

import model.Event;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TestWindow1 extends AbstractTestMain11{
	public static void main(String[] args) {

	}

	public static void testMethod1() {
		DataStreamSource<Event> dataStreamSource = env.fromCollection(getTestMain11data());
//		dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
//			@Override
//			public long extractAscendingTimestamp(Event element) {
//				return element.getTime();
//			}
//		}).keyBy("a").window(TumblingEventTimeWindows.of(Time.minutes(1),Time.milliseconds(100))).apply();
	}

}
