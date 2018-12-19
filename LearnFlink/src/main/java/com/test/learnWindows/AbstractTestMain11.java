package com.test.learnWindows;

import model.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractTestMain11 extends AbstractTestMain1 {
	public static List<StreamRecord> getTestMain11data(){
		List<StreamRecord> inputEvents = new ArrayList<>(); // 构建数据源
		Event event0 = new Event(0, "x", 1.0);
		Event event1 = new Event(1, "a", 1.0);
		Event event2 = new Event(2, "b", 2.0);
		Event event3 = new Event(3, "c", 3.0);
		Event event4 = new Event(4, "a", 4.0);
		Event event5 = new Event(5, "b", 5.0);

		inputEvents.add(new StreamRecord<>(event0, 3));
		inputEvents.add(new StreamRecord<>(event1, 3));
		inputEvents.add(new StreamRecord<>(event2, 3));
		inputEvents.add(new StreamRecord<>(event3, 3));
		inputEvents.add(new StreamRecord<>(event4, 3));
		inputEvents.add(new StreamRecord<>(event5, 3));
		return inputEvents;
	}
}
