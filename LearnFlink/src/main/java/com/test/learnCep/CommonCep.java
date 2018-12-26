package com.test.learnCep;

import com.test.customAbstract.AbstractTestSocket;
import com.test.sink.CustomPrintEvent;
import model.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CommonCep {
	public static void timeOut(DataStream<Event> dataStream, Pattern<Event,Event> pattern,String patternS){
		Pattern<Event,Event> pattern1 = pattern.within(Time.seconds(10));
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern1);
		OutputTag<Event> outputTag = new OutputTag<Event>("side-output"){};
		SingleOutputStreamOperator<Event> dataStream1 = patternStream.flatSelect(outputTag, new PatternFlatTimeoutFunction<Event, Event>() {
			@Override
			public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {
				System.out.println("3秒钟延迟事件");
				if (patternS != null){
					List<Event> list = pattern.get(patternS);
					if (list != null){
						Iterator<Event> iterator  = list.iterator();
						while (iterator.hasNext()){
							Event event = iterator.next();
							out.collect(event);
						}
					}
				}else {
					Iterator<Map.Entry<String,List<Event>>> iterator = pattern.entrySet().iterator();
					while (iterator.hasNext()){
						Map.Entry<String,List<Event>> entry = iterator.next();
						List<Event> list = entry.getValue();
						if (list != null){
							Iterator<Event> iteratorList  = list.iterator();
							while (iteratorList.hasNext()){
								Event event = iteratorList.next();
								event.setPatternName(entry.getKey());
								out.collect(event);
							}
						}
					}
				}

			}
		}, new PatternFlatSelectFunction<Event, Event>() {
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
				System.out.println("未延迟事件");
				if (patternS != null){
					List<Event> list = pattern.get(patternS);
					if (list != null){
						Iterator<Event> iterator  = list.iterator();
						while (iterator.hasNext()){
							Event event = iterator.next();
							out.collect(event);
						}
					}
				}else {
					Iterator<Map.Entry<String,List<Event>>> iterator = pattern.entrySet().iterator();
					while (iterator.hasNext()){
						Map.Entry<String,List<Event>> entry = iterator.next();
						List<Event> list = entry.getValue();
						if (list != null){
							Iterator<Event> iteratorList  = list.iterator();
							while (iteratorList.hasNext()){
								Event event = iteratorList.next();
								event.setPatternName(entry.getKey());
								out.collect(event);
							}
						}
					}
				}
			}
		});

		DataStream<Event> dataStream2 = dataStream1.getSideOutput(outputTag);
		dataStream2.addSink(new CustomPrintEvent("test.txt")).setParallelism(1);
		dataStream1.addSink(new CustomPrintEvent("test1.txt")).setParallelism(1);
	}

	public static void simplePattern(DataStream<Event> dataStream,Pattern<Event,Event> pattern,String patternS){
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern);
		DataStream<Event> dataStream1 = patternStream.flatSelect(new PatternFlatSelectFunction<Event, Event>() {
			int numberofTimes = 0;
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
				System.out.println("满足要求事件");
				numberofTimes ++;
				if (patternS != null){
					List<Event> list = pattern.get(patternS);
					if (list != null){
						Iterator<Event> iterator  = list.iterator();
						while (iterator.hasNext()){
							Event event = iterator.next();
							event.setNumberOfTimes(numberofTimes);
							out.collect(event);
						}
					}else {
						Event event = new Event(0,"0",0D,0L);
						event.setNumberOfTimes(numberofTimes);
					}
				}else {
					Iterator<Map.Entry<String,List<Event>>> iterator = pattern.entrySet().iterator();
					while (iterator.hasNext()){
						Map.Entry<String,List<Event>> entry = iterator.next();
						List<Event> list = entry.getValue();
						if (list != null){
							Iterator<Event> iteratorList  = list.iterator();
							while (iteratorList.hasNext()){
								Event event = iteratorList.next();
								event.setPatternName(entry.getKey());
								event.setNumberOfTimes(numberofTimes);
								out.collect(event);
							}
						}
					}
				}

			}
		});
		dataStream1.addSink(new CustomPrintEvent("test.txt")).setParallelism(1);
	}
}
