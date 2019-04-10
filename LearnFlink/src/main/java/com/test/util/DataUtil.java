package com.test.util;

import model.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.ArrayList;
import java.util.List;

public class DataUtil {
	public static List<Tuple2<Long,String>> getTuple2(){
		List<Tuple2<Long,String>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,String> tuple2 = new Tuple2(date,"aa");
			list.add(tuple2);
		}
		return list;
	}

	public static List<Tuple2<Long,Boolean>> getTuple2_Boolean(){
		List<Tuple2<Long,Boolean>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,Boolean> tuple2 = new Tuple2(date,true);
			list.add(tuple2);
		}
		return list;
	}
	public static List<Tuple2<Long,Integer>> getTuple2_Int(){
		List<Tuple2<Long,Integer>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,Integer> tuple2 = new Tuple2(date,i);
			list.add(tuple2);
		}
		return list;
	}

	public static List<Tuple3<Long,Integer,String>> getTuple3_Int(){
		List<Tuple3<Long,Integer,String>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple3<Long,Integer,String> tuple3 = new Tuple3(date,i,"aa");
			list.add(tuple3);
		}
		return list;
	}

	/**
	 * @param a 从0到a 创造的数据
	 * @param b 从a到b 创造的数据
	 * @param num 总的数据
	 * @return
	 */
	public static List<Event> getEventCollection(int a, int b, int num) {
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, date);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(60, "a", 2.0, date);
				inputEvents.add(event);
			} else {
				date = date + 10000;
				Event event = new Event(i, "c", 3.0, date);
				inputEvents.add(event);
			}
		}
		return inputEvents;
	}

	public static List<Event> getEventCollection2() {
		int a = 1;int b = 2;int num = 4;
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, date);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(60, "b", 2.0, date);
				inputEvents.add(event);
			} else {
				date = date + 10000;
				Event event = new Event(70, "c", 3.0, date);
				inputEvents.add(event);
			}
		}
		return inputEvents;
	}
}
