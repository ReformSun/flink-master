package com.test.learnCep;

import com.test.customAbstract.AbstractTestSocket;
import com.test.learnWindows.AbstractTestMain1;
import com.test.learnWindows.AbstractTestMain11;
import com.test.sink.CustomPrintEvent;
import model.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 用户
 */
public class TestMain1 extends AbstractTestSocket {
	public static void main(String[] args) {
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
			DataStream<Event> dataStreamSource1 = getInput();

			testMethod1_1(dataStreamSource1);
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("test11");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void testMethod1_1(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).followedBy("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		testMethod1(dataStream,pattern);
//		testMethod2(dataStream,pattern);
	}

	public static void testMethod1(DataStream<Event> dataStream,Pattern<Event,Event> pattern){
		Pattern<Event,Event> pattern1 = pattern.within(Time.seconds(10));
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern1);
		OutputTag<Event> outputTag = new OutputTag<Event>("side-output"){};
		SingleOutputStreamOperator<Event> dataStream1 = patternStream.flatSelect(outputTag, new PatternFlatTimeoutFunction<Event, Event>() {
			@Override
			public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {
				System.out.println("3秒钟延迟事件");
				List<Event> list = pattern.get("second");
				if (list != null && list.size() != 0){
					Iterator<Event> iterator  = list.iterator();
					while (iterator.hasNext()){
						Event event = iterator.next();
						out.collect(event);
					}
				}

			}
		}, new PatternFlatSelectFunction<Event, Event>() {
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
				System.out.println("未延迟事件");
				List<Event> list = pattern.get("second");
				if (list != null){
					Iterator<Event> iterator  = list.iterator();
					while (iterator.hasNext()){
						Event event = iterator.next();
						out.collect(event);
					}
				}
			}
		});

		DataStream<Event> dataStream2 = dataStream1.getSideOutput(outputTag);
		dataStream2.addSink(new CustomPrintEvent()).setParallelism(1);
		dataStream1.addSink(new CustomPrintEvent("test1.txt")).setParallelism(1);

	}

	public static void testMethod2(DataStream<Event> dataStream,Pattern<Event,Event> pattern){
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern);
		DataStream<Event> dataStream1 = patternStream.flatSelect(new PatternFlatSelectFunction<Event, Event>() {
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
				System.out.println("满足要求事件");
				List<Event> list = pattern.get("second");
				if (list != null){
					Iterator<Event> iterator  = list.iterator();
					while (iterator.hasNext()){
						Event event = iterator.next();
						out.collect(event);
					}
				}
			}
		});
		dataStream1.addSink(new CustomPrintEvent()).setParallelism(1);
	}
}
