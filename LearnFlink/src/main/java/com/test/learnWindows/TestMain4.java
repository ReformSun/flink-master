package com.test.learnWindows;

import com.test.flatMap_1.SunFunctionStates1;
import com.test.sink.CustomPrint;
import com.test.util.FileWriter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;

import java.io.IOException;

/**
 * 理解keyby key相当于一个sql中的分组 flink在生成
 */
public class TestMain4 {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.enableCheckpointing(60000);
//		env.setStateBackend(new RocksDBStateBackend("file:///Users/apple/Desktop/rockdata"));
//		testMethod1(env);
//		testMethod2(env);
//		testMethod3(env);
		testMethod4(env);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamExecutionEnvironment env) {
		DataStreamSource<Tuple2<Long,Long>> input = env.fromElements(Tuple2.of(1L,3L), Tuple2.of(2L,5L), Tuple2.of(1L,3L), Tuple2.of(1L,7L));
		DataStream<String> dataStream = input.keyBy(1).flatMap(new FlatMapFunction<Tuple2<Long,Long>, String>() {
			@Override
			public void flatMap(Tuple2<Long,Long> value, Collector<String> out) throws Exception {
				out.collect(value.getField(1).toString());
			}
		});
		dataStream.addSink(new CustomPrint("test1.txt"));

//		DataStream<String> dataStream1 = input.keyBy(2).flatMap(new FlatMapFunction<Tuple2<Long,Long>, String>() {
//			@Override
//			public void flatMap(Tuple2<Long,Long> value, Collector<String> out) throws Exception {
//				out.collect(value.getField(1).toString());
//			}
//		});
//		dataStream1.addSink(new CustomPrint("test2.txt"));
	}

	public static void testMethod2(StreamExecutionEnvironment env) {
		DataStreamSource<String> input = env.fromElements("a","b","c");
		DataStream<String> dataStream = input.keyBy(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
			}
		});
		dataStream.addSink(new CustomPrint("test1.txt"));
	}

	public static void testMethod3(StreamExecutionEnvironment env) {
		DataStreamSource<SunWordWithCount> input = env.fromElements(new SunWordWithCount("a",1),new SunWordWithCount("a",1),new SunWordWithCount("a",1),new SunWordWithCount("d",1));
		DataStream<SunWordWithCount> dataStream = input.keyBy("word")
			.reduce(new ReduceFunction<SunWordWithCount>() {
				@Override
				public SunWordWithCount reduce(SunWordWithCount a, SunWordWithCount b) {
					return new SunWordWithCount(a.word,a.count + b.count);
				}
			});
		dataStream.addSink(new Print2());
	}

	public static void testMethod4(StreamExecutionEnvironment env) {
		DataStreamSource<SunWordWithCount> input = env.fromElements(new SunWordWithCount("a",1),new SunWordWithCount("a",1),new SunWordWithCount("a",1),new SunWordWithCount("d",1)).setParallelism(1);
		DataStream<SunWordWithCount> dataStream = input.keyBy(new KeySelector<SunWordWithCount, String>() {
			@Override
			public String getKey(SunWordWithCount value) throws Exception {
				return value.word;
			}
		})
			.reduce(new ReduceFunction<SunWordWithCount>() {
				@Override
				public SunWordWithCount reduce(SunWordWithCount a, SunWordWithCount b) {
					return new SunWordWithCount(a.word,a.count + b.count);
				}
			}).setParallelism(1);
		dataStream.addSink(new Print2()).setParallelism(1);
	}

	public static void testMethod5() {
		
	}

}



class Print2 extends RichSinkFunction<SunWordWithCount> {
	@Override
	public void invoke(SunWordWithCount value) throws Exception {
		FileWriter.writerFile(value.count +"count " +value.word ,"test1.txt");
	}
}


