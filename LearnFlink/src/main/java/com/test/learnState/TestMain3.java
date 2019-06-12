package com.test.learnState;

import com.test.filesource.FileSourceTuple3;
import com.test.sink.CustomPrintTuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.IOException;

public class TestMain3 {

	public static void main(String[] args) throws IOException {
		final CustomStreamEnvironment env = new CustomStreamEnvironment();
        testMethod1(env);
//		testMethod2(env);
//		StreamGraph streamGraph = env.getStreamGraph();
//		System.out.println(streamGraph.getStreamingPlanAsJSON());
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamExecutionEnvironment env) {
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.keyBy(0).sum(1).addSink(new CustomPrintTuple("test.txt"));
	}

	public static void testMethod2(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.keyBy(0).sum(1).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
//					throw new Exception("test");
				}else {
					System.out.println(value.toString());
				}
			}
		});
	}
}
