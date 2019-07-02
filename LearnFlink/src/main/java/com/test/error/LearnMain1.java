package com.test.error;

import com.test.sink.CustomSocketSink;
import com.test.source.CustomSource;
import com.test.source.ProduceTuple3;
import com.test.source.ProduceTuple3_v1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;

public class LearnMain1 {
	public static void main(String[] args) throws IOException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(6000);
//        testMethod1(env);
//        testMethod2(env);
//        testMethod3(env);
//        testMethod4(env);
		testMethod5(env);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env
			.addSource(new CustomSource<>(1000,new ProduceTuple3()))
			.setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
					throw new IllegalArgumentException("testMap");
				}
				return value;
			}
		}).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
					throw new Exception("test");
				}else {
					System.out.println(value.toString());
				}
			}
		});
	}

	public static void testMethod2(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env
			.addSource(new CustomSource<>(1000,new ProduceTuple3()))
			.setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				return null;
			}
		}).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				System.out.println(value.toString());
			}
		});
	}

	/**
	 * 返回的数据为null时
	 * 在本地测试不会报错
	 * 放到测试环境测试
	 * 报错 但是job任务并不会挂掉，数据无法跳过错误数据继续进行消费只能堵塞
	 * 重新创建，创建完成，重新创建，
	 * @param env
	 */
	public static void testMethod3(StreamExecutionEnvironment env){
		String host = "10.4.247.17";
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env
			.addSource(new CustomSource<>(1000,new ProduceTuple3()))
			.setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				if (value.f0.equals("b")){
					return null;
				}else {
					return value;
				}

			}
		}).addSink(new CustomSocketSink<>(null)).setParallelism(1);
	}




	/**
	 * 直接扔出异常给flink处理
	 *
	 * @param env
	 */
	public static void testMethod4(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env
			.addSource(new CustomSource<>(1000,new ProduceTuple3()))
			.setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				if (value.f0.equals("b")){
					throw new Exception("aaaa");
				}else {
					return value;
				}

//				return value;

			}
		}).addSink(new CustomSocketSink<>(null));
	}

	/**
	 * 在source中直接发送null数据，也是会出现报错，job任务陷入重新启动，成功，重新启动的循环中
	 * @param env
	 */
	public static void testMethod5(StreamExecutionEnvironment env){
		String host = "10.4.247.17";
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env
			.addSource(new CustomSource<>(1000,new ProduceTuple3_v1()))
			.setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				return value;

			}
		}).addSink(new CustomSocketSink<>(null)).setParallelism(1);
	}
}
