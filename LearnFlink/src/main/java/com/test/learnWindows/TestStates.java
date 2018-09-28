package com.test.learnWindows;

import com.test.flatMap_1.SunFunctionStates1;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;

import java.io.IOException;

public class TestStates {
    public static void main(String[] args) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setStateBackend(new RocksDBStateBackend("file:///C:\\Users\\xiaorong\\Desktop\\rockdata"));
//        testMethod1(env);
		testMethod2(env);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	public static void testMethod1(StreamExecutionEnvironment env) {
		env.fromElements(Tuple2.of(1L,3L), Tuple2.of(1L,5L), Tuple2.of(1L,3L), Tuple2.of(1L,7L))
			.keyBy(0)
			.flatMap(new SunFunctionStates1())
			.print().setParallelism(1);
	}

	public static void testMethod2(StreamExecutionEnvironment env) {
		DataStreamSource<String> input = env.addSource(KafkaUtil.getKafkaTableSource("wordcount"));
		DataStream<SunWordWithCount> dataStream = input.flatMap(new FlatMapFunction<String, SunWordWithCount>() {
			@Override
			public void flatMap(String value, Collector<SunWordWithCount> out) throws Exception {
				for (String ss:value.split("\\|{1}")){
					out.collect(
						new SunWordWithCount(ss,1)
					);
				}
			}
		});

		DataStream<SunWordWithCount> dataStream1 = dataStream.keyBy(new KeySelector<SunWordWithCount, Object>() {
			@Override
			public Object getKey(SunWordWithCount value) throws Exception {
				return value.word;
			}
		}).reduce(new RichReduceFunction<SunWordWithCount>() {
			@Override
			public SunWordWithCount reduce(SunWordWithCount value1, SunWordWithCount value2) throws Exception {
				return new SunWordWithCount(value1.word,value1.count + value2.count);
			}
		});

		dataStream1.addSink(new CustomWordCountPrint());

	}
}
