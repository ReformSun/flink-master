package com.test.learnWindows;

import com.test.sink.CustomPrint;
import com.test.sink.CustomPrintTuple2;
import model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.Map;

public class TestMain13 extends AbstractTestMain11{
	public static void main(String[] args) {
//		testMethod1();
//		testMethod2();
		testMethod3();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1() {
		DataStream<String> dataStream = getInput("assdddd");
		dataStream.addSink(new CustomPrint("test.txt"));
	}

	public static void testMethod2() {
		List<Tuple2<String,Long>> inputEvents = getTestMain12data();
		DataStream<Tuple2<String,Long>> dataStreamSource1 = env.fromCollection(inputEvents).setParallelism(1);
		dataStreamSource1.keyBy(new KeySelector<Tuple2<String,Long>, String>() {
			@Override
			public String getKey(Tuple2<String, Long> value) throws Exception {
				return value.f0;
			}
		}).sum("f1").addSink(new CustomPrintTuple2());
	}

	public static void testMethod3() {
		DataStream<String> dataStream = getInputSocket(9000);
		dataStream.map(new MapFunction<String, Tuple2<String,Double>>() {
			@Override
			public Tuple2<String,Double> map(String value) throws Exception {
				Map<String,Object> map = gson.fromJson(value,Map.class);
				if (map.get("f0").equals("b")){
					throw new ClassNotFoundException("ddd");
				}
				return new Tuple2<String,Double>((String) map.get("f0"),(Double) map.get("f1"));
			}
		}).keyBy(new KeySelector<Tuple2<String,Double>, String>() {
			@Override
			public String getKey(Tuple2<String, Double> value) throws Exception {
				return value.f0;
			}
		}).sum("f1").addSink(new CustomPrintTuple2());
	}
}
