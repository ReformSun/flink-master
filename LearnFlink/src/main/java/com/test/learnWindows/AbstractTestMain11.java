package com.test.learnWindows;

import model.Event;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractTestMain11 extends AbstractTestMain1 {
	public static List<Event> getTestMain11data(){
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;

		for (int i = 0; i < 5; i++) {

			if (i % 2 == 0){
				date = date + 20000;
				Event event = new Event(i, "a", 1.0,date);
				inputEvents.add(event);
			}else if (i < 4){
				date = date + 10000;
				Event event = new Event(i, "b", 2.0,date);
				inputEvents.add(event);
			}else {
				date = date + 10000;
				Event event = new Event(i, "c", 3.0,date);
				inputEvents.add(event);
			}
		}
		return inputEvents;
	}

	public static List<Tuple2<String,Long>> getTestMain12data(){
		List<Tuple2<String,Long>> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;

		for (int i = 0; i < 5; i++) {
			if (i % 2 == 0){
				date = date + 20000;
				inputEvents.add(new Tuple2<>("a",date));
			}else if (i < 4){
				date = date + 10000;
				inputEvents.add(new Tuple2<>("a",date));
			}else {
				date = date + 10000;
				inputEvents.add(new Tuple2<>("a",date));
			}
		}
		return inputEvents;
	}

	/**
	 * 测试数据生成
	 * @param a 第一批类型的数据个数
	 * @param b 第二批数据的个数
	 * @param num 总的数据个数
	 * @return
	 */
	public static List<Tuple2<String,Long>> getTestSplitStream(int a ,int b, int num){
		List<Tuple2<String,Long>> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;

		for (int i = 0; i < num; i++) {
			if (a > i){
				date = date + 20000;
				inputEvents.add(new Tuple2<>("a",date));
			}else if (b + a > i){
				date = date + 10000;
				inputEvents.add(new Tuple2<>("b",date));
			}else {
				date = date + 10000;
				inputEvents.add(new Tuple2<>("c",date));
			}
		}
		return inputEvents;
	}



}
