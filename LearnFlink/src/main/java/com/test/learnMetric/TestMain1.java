package com.test.learnMetric;

import com.test.filesource.FileSourceTuple3;
import com.test.learnMetric.function.TestRichFlatMapFunction;
import com.test.sink.CustomPrint;
import com.test.sink.CustomPrintTuple3;
import com.test.util.DataUtil;
import com.test.util.StreamExecutionEnvUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class TestMain1 {
	private static final StreamExecutionEnvironment env;
	static {
		env = StreamExecutionEnvUtil.getStreamExecutionEnvironment(null);
	}
	public static void main(String[] args) {
		try{
//			testMethod1();
			testMethod2();
//			testMethod3();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("LearnMetric");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getTuple3_Int_timetamp(5,6,18);
		DataStreamSource<Tuple3<String,Integer,Long>> dataStreamSource = env.fromCollection(list);
		dataStreamSource.map(new MapFunction<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String map(Tuple3<String, Integer, Long> value) throws Exception {
				return value.toString();
			}
		}).addSink(new CustomPrint(null));
	}

	public static void testMethod2(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getTuple3_Int_timetamp(5,6,18);
		DataStreamSource<Tuple3<String,Integer,Long>> dataStreamSource = env.fromCollection(list);
		dataStreamSource.flatMap(new TestRichFlatMapFunction()).addSink(new CustomPrint(null));
	}

	public static void testMethod3(){
		DataStreamSource<Tuple3<String,Integer,Long>> dataStreamSource = env.addSource(new FileSourceTuple3(10000));
		dataStreamSource.flatMap(new TestRichFlatMapFunction()).addSink(new CustomPrint(null));
	}
}
