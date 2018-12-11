package com.test.learnWindows;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.customAssignTAndW.CustomAssignerTimestampsAndWatermark;
import com.test.flatMap_1.FlatMapFunctionTimeAndNumber;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;
import test.TimeAndNumber;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 学习flink的jion操作
 * org.apache.flink.runtime.state.heap.HeapListState
 * org.apache.flink.streaming.api.datastream.CoGroupedStreams.CoGroupWindowFunction
 * org.apache.flink.streaming.api.datastream.JoinedStreams.JoinCoGroupFunction
 * org.apache.flink.streaming.api.datastream.CoGroupedStreams
 *org.apache.flink.runtime.state.heap.HeapPriorityQueue
 *
 *
 * 学习jion的操作 必须直到coGroup的原理
 * 而coGroup的实现又是基于union和keyStream实现的
 *
 * 当使用join时
 * WindowOperator 的保存操作状态的模型是HeapListState
 * HeapListState可以保存一个key只对应多个值的情况
 *
 * jion 是建立在cogroup上的
 * 具体实现细节在CoGroupedStreams中的apply(CoGroupFunction<T1, T2, T> function, TypeInformation<T> resultType)方法中
 *
 * 逻辑
 * 首先把两个流自己通过一个map处理 两个流分别被两个Input1Tagger 和Input2Tagger 处理 就是把数据包装了一下
 * 包装为TaggedUnion 他可以把数据标注成第一个流的或者是第个流的
 *
 * 然后窗口触发后会把窗口内的所有数据发射
 *
 * 但是会先经过CoGroupWindowFunction类的apply(KEY key,W window,Iterable<TaggedUnion<T1, T2>> values,Collector<T> out)方法预处理
 * 逻辑：把流一和流二的数据分别放到不同的容器中，传给下一个操作
 *
 * 下一个操作为JoinCoGroupFunction类的public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {方法
 * 逻辑：举例：假如一的数据为：1,2,3 二的流 3，4,5 转给下一级的组合为
 * 1,3,
 * 1,4
 * 1,5
 * 2,3
 * 2,4
 * 2,5
 * 3,3
 * 3,4
 * 3,5
 *
 * 关键问题怎么实现的在一个窗口内相等的指定key值的做处理
 *
 *
 *
 *
 *
 *
 *
 */
public class TestMain7 extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
//			testMethod1(getInput(),getInput2());
			testMethod3();
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

	public static void testMethod3(){

		List<Tuple3<String,Integer,Long>> list = getTestdata();
	    DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource2 = env.fromCollection(list).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);

		JoinedStreams<Tuple3<String,Integer,Long>,Tuple3<String,Integer,Long>> joinedStreams = dataStreamSource1.join(dataStreamSource2);

		JoinedStreams.Where where = joinedStreams.where(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer,Long> value) throws Exception {
				return value.getField(0);
			}
		});

		JoinedStreams.Where.EqualTo equalTo = where.equalTo(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String,Integer,Long> value) throws Exception {
				return value.getField(0);
			}
		});

		equalTo.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0))).apply(new JoinFunction<Tuple3<String,Integer,Long>,Tuple3<String,Integer,Long>,Tuple3<String,
			Integer,Long>>() {
			@Override
			public Tuple3<String, Integer,Long> join(Tuple3<String, Integer,Long> first, Tuple3<String, Integer,Long> second) throws Exception {
				return new Tuple3<String,Integer,Long>(first.getField(0),(Integer) first.getField(1) + (Integer)first.getField(1),first.getField(2));
			}
		}).addSink(new CustomPrintTuple3()).setParallelism(1);

//		equalTo.window(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply((Tuple3<String, Integer,Long> first, Tuple3<String, Integer,Long> second) -> {
//			try{
//				return new Tuple3<String,Integer,Long>(first.getField(0),(Integer) first.getField(1) + (Integer)first.getField(1),first.getField(2));
//
//			}catch (Exception e){
//			    e.printStackTrace();
//			}
//		}).print()
//			.setParallelism(1);


	}








}
