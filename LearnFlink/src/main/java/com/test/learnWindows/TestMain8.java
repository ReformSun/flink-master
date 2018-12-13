package com.test.learnWindows;

import com.test.flatMap_1.FlatMapFunctionTimeAndNumber;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomTimeAndNumberSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import test.TimeAndNumber;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 学习flink的uniom操作
 */
public class TestMain8 extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
//			testMethod1(input,input2);
//			testMethod2(getInput(),getInput2());
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
	 * 两个流变成一个流 两个流的数据都会打印出来
	 * @param input
	 * @param input2
	 */
	public static void testMethod1(DataStreamSource<String> input,DataStreamSource<String> input2) {
		DataStream<TimeAndNumber> dataStream1 = input.flatMap(new FlatMapFunctionTimeAndNumber());
		DataStream<TimeAndNumber> dataStream2 = input2.flatMap(new FlatMapFunctionTimeAndNumber());
		DataStream<TimeAndNumber> dataStream = dataStream1.union(dataStream2);
		dataStream.addSink(new CustomTimeAndNumberSink()).setParallelism(1);
	}

	/**
	 * org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
	 * org.apache.flink.runtime.state.heap.HeapKeyedStateBackend
	 * org.apache.flink.streaming.api.operators.StreamGroupedReduce
	 * org.apache.flink.runtime.state.heap.HeapValueState
	 * org.apache.flink.streaming.runtime.io.StreamInputProcessor
	 * org.apache.flink.streaming.api.operators.AbstractStreamOperator
	 * keyedStream分区的原理
	 * 当使用keyedStream.reduce时
	 * flink 就会生成一个HeapKeyedStateBackend的状态后端
	 * 当StreamGroupedReduce被初始化并调用open方法时，会针对此操作类获得ValueState
	 * ValueState是通过HeapKeyedStateBackend的createInternalState方法创建 这里ValueState为HeapValueState类型
	 * HeapValueState通过类CopyOnWriteStateTable操作后台的状态表 表是key value类型
	 *
	 *
	 * 当每一次上一个任务生成的元素传过来通过StreamInputProcessor的processInput方法处理
	 * 回调用StreamGroupedReduce.setKeyContextElement1(record);方法
	 *
	 * 然后可以进到AbstractStreamOperator类查看setKeyContextElement1方法的具体实现
	 * 实现逻辑：setKeyContextElement1的实现是 从record中获取用户自定义的key值，然后存储到状态后端中
	 *
	 * 然后StreamInputProcessor的processInput方法调用StreamGroupedReduce.processElement(record)方法
	 * 这是可以查看StreamGroupedReduce类的processElement方法实现
	 *  实现逻辑：通过当前值的key从状态后端中获得对应的值
	 *
	 *  转给给下一步操作
	 *
	 *  执行完成后更新状态后端内的值
	 *
	 *  此时窗台后端的当前key就是次record的值 因为setKeyContextElement1操作在前
	 *
	 *
	 *
	 *
	 * @param input
	 * @param input2
	 */
	public static void testMethod2(DataStreamSource<String> input,DataStreamSource<String> input2) {
		DataStream<TimeAndNumber> dataStream1 = input.flatMap(new FlatMapFunctionTimeAndNumber());
		DataStream<TimeAndNumber> dataStream2 = input2.flatMap(new FlatMapFunctionTimeAndNumber());

		DataStream<TimeAndNumber> dataStream = dataStream1.union(dataStream2);
		KeyedStream<TimeAndNumber,Long> keyedStream = dataStream.keyBy(new KeySelector<TimeAndNumber, Long>() {
			@Override
			public Long getKey(TimeAndNumber value) throws Exception {
				return value.getTimestamp();
			}
		});
		keyedStream.reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return new TimeAndNumber(value1.getTimestamp(),value1.getNumber() + value2.getNumber());
			}
		}).addSink(new CustomTimeAndNumberSink());
	}

	public static void testMethod3() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list);

		DataStream dataStream = dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.getField(0);
			}
		}).reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String,Integer,Long> value2) throws Exception {
				Integer integer1 = value1.getField(1);
				Integer integer2 = value2.getField(1);

				return new Tuple3<String, Integer, Long>(value1.getField(0),integer1.intValue() + integer2.intValue(),value1.getField(2));
			}
		}).setParallelism(4);

		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}
}
