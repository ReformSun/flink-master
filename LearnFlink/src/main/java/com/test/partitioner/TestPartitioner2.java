package com.test.partitioner;

import com.test.aggregator.SumAggregator;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.filesource.FileSourceTuple3;
import com.test.learnState.CustomStreamEnvironment;
import com.test.learnTableapi.FileUtil;
import com.test.util.DataUtil;
import com.test.util.FileWriter;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class TestPartitioner2 {
	static final CustomStreamEnvironment env = new CustomStreamEnvironment();
	public static void main(String[] args) {
//		testMethod1();
//		testMethod1_1();
		testMethod1_2();
//		testMethod1_3();
//		testMethod2();
//		testMethod3();
//		testMethod4();
//		testMethod5();
//		testMethod6();
//		testMethod6_1();
		StreamGraph streamGraph = env.getStreamGraph();
		System.out.println(streamGraph.getStreamingPlanAsJSON());

		start();
	}

	private static void start(){
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * {@link org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner}
	 */
	public static void testMethod1(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(1);
		dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String,Integer,Long> value) throws Exception {
				return value.f0;
			}
		}).sum("1").setParallelism(3).rescale().addSink(new CustomSink()).setParallelism(2);
	}

	public static void testMethod1_3(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0))
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(2)).setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>, String> keyedStream = dataStreamSource1.rebalance().keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String,Integer,Long> value) throws Exception {
				return value.f0;
			}
		});
		keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60)))
			.reduce(new SumAggregator<>("1",keyedStream.getType(),keyedStream.getExecutionConfig()))
			.setParallelism(3)
			.addSink(new CustomSink());
	}

	public static void testMethod1_2(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0))
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(2)).setParallelism(1);
		dataStreamSource1.rebalance().keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String,Integer,Long> value) throws Exception {
				return value.f0;
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(60))).sum("1").setParallelism(1)
			.rescale().addSink(new CustomSink()).setParallelism(1);
	}

	public static void testMethod1_1(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0))
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(2)).setParallelism(1);
		dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String,Integer,Long> value) throws Exception {
				return value.f0;
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(60))).sum("1").setParallelism(3)
			.rescale().addSink(new CustomSink()).setParallelism(2);
	}

	/**
	 * {@link org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner}
	 */
	public static void testMethod2(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(3);
		dataStreamSource1.forward().addSink(new CustomSink()).setParallelism(3);
	}
	/**
	 * {@link org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner}
	 */
	public static void testMethod3(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(1);
		dataStreamSource1.rebalance().addSink(new CustomSink()).setParallelism(3);
	}
	/**
	 * {@link org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner}
	 * 把数据广播到所有的通道中
	 */
	public static void testMethod4(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(1);
		dataStreamSource1.broadcast().addSink(new CustomSink()).setParallelism(3);
	}
	/**
	 * {@link org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner}
	 * 随机选择一个通道发送数据
	 */
	public static void testMethod5(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(1);
		dataStreamSource1.shuffle().addSink(new CustomSink()).setParallelism(3);
	}

	/**
	 * 对于通道轮训的形式发送
	 * {@link org.apache.flink.streaming.runtime.partitioner.RescalePartitioner}
	 */
	public static void testMethod6(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0)).setParallelism(1);
		dataStreamSource1.rescale().addSink(new CustomSink()).setParallelism(3);
	}
	public static void testMethod6_1(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(0))
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3<>(2))
			.setParallelism(1);
		dataStreamSource1.rescale().windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
			.sum(1).addSink(new CustomSink()).setParallelism(2);
	}
}

class CustomSink extends RichSinkFunction<Tuple3<String,Integer,Long>>{
	private String taskName;
	@Override
	public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
		FileWriter.writerFile(value.toString() + " :" + taskName,"test.txt");
	}

	@Override
	public void open(Configuration parameters) throws Exception {
//		taskName = getRuntimeContext().getTaskName();
		taskName = getRuntimeContext().getTaskNameWithSubtasks();
		super.open(parameters);
	}
}

class CustomReduce extends RichReduceFunction<Tuple3<String,Integer,Long>>{
	@Override
	public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
		return null;
	}
}
