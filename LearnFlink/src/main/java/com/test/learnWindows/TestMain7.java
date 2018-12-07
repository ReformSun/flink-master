package com.test.learnWindows;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.customAssignTAndW.CustomAssignerTimestampsAndWatermark;
import com.test.flatMap_1.FlatMapFunctionTimeAndNumber;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;
import test.TimeAndNumber;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 学习flink的jion操作
 *
 * 学习jion的操作 必须直到coGroup的原理
 * 而coGroup的实现又是基于union和keyStream实现的
 */
public class TestMain7 extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
			testMethod1(input,input2);
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 两个普通的任务的join
	 * @param input
	 * @param input2
	 */
	public static void testMethod1(DataStreamSource<String> input,DataStreamSource<String> input2) {
		DataStream<TimeAndNumber> dataStream = input.flatMap(new FlatMapFunctionTimeAndNumber()).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark());
		DataStream<TimeAndNumber> dataStream2 = input2.flatMap(new FlatMapFunctionTimeAndNumber()).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark());

		 JoinedStreams<TimeAndNumber,TimeAndNumber> joinedStreams = dataStream.join(dataStream2);

		 JoinedStreams.Where where = joinedStreams.where(new KeySelector<TimeAndNumber, Long>() {
			 @Override
			 public Long getKey(TimeAndNumber value) throws Exception {
				 return value.getTimestamp();
			 }
		 });
		JoinedStreams.Where.EqualTo equalTo = where.equalTo(new KeySelector<TimeAndNumber, Long>() {
			 @Override
			 public Long getKey(TimeAndNumber value) throws Exception {
				 return value.getTimestamp();
			 }
		 });
		DataStream<TimeAndNumber> dataStream3 = equalTo.window(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply(new JoinFunction<TimeAndNumber,TimeAndNumber,TimeAndNumber>() {
			@Override
			public TimeAndNumber join(TimeAndNumber first, TimeAndNumber second) throws Exception {
				return new TimeAndNumber(first.getTimestamp(),first.getNumber() + second.getNumber());
			}
		});
		dataStream3.addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				java.nio.file.Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}

	public static void testMethod2(DataStreamSource<String> input,DataStreamSource<String> input2) {
		DataStream<TimeAndNumber> dataStream = input.flatMap(new FlatMapFunctionTimeAndNumber()).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark());
		DataStream<TimeAndNumber> dataStream2 = input2.flatMap(new FlatMapFunctionTimeAndNumber()).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark());
		CoGroupedStreams<TimeAndNumber,TimeAndNumber> coGroupedStreams = dataStream.coGroup(dataStream2);
		CoGroupedStreams.Where where = coGroupedStreams.where(new KeySelector<TimeAndNumber, Long>() {
			@Override
			public Long getKey(TimeAndNumber value) throws Exception {
				return value.getTimestamp();
			}
		});

		CoGroupedStreams.Where.EqualTo equalTo = where.equalTo(new KeySelector<TimeAndNumber, Long>() {
			@Override
			public Long getKey(TimeAndNumber value) throws Exception {
				return value.getTimestamp();
			}
		});

		equalTo.window(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply(new CoGroupFunction<TimeAndNumber,TimeAndNumber,TimeAndNumber>() {
			@Override
			public void coGroup(Iterable<TimeAndNumber> first, Iterable<TimeAndNumber> second, Collector<TimeAndNumber> out) throws Exception {

			}
		});
	}



}
