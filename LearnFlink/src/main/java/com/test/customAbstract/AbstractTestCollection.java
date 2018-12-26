package com.test.customAbstract;

import model.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.ArrayList;
import java.util.List;

public class AbstractTestCollection extends AbstractTestCommon {
	public static DataStream<Event> getInputFromCollection(int a, int b, int num) {
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, i+1);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(i, "b", 2.0, date);
				inputEvents.add(event);
			} else {
				date = date + 10000;
				Event event = new Event(i, "c", 3.0, date);
				inputEvents.add(event);
			}
		}
		DataStreamSource<Event> input =  env.fromCollection(inputEvents);
		return input;
	}

	public static DataStream<Event> getInputFromCollection(int a,int b,int num,int a2) {
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534473000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, i+1);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(i, "b", 2.0, i + 1);
				inputEvents.add(event);
			} else if ( i < a2) {
				date = date + 10000;
				Event event = new Event(50, "a", i, i+1);
				inputEvents.add(event);
			}else {
				date = date + 10000;
				Event event = new Event(i, "c", 3.0, i + 1);
				inputEvents.add(event);
			}
		}
		DataStreamSource<Event> input =  env.fromCollection(inputEvents);
		return input;
	}
}
