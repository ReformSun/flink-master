package com.test.learnWindows;

import com.sun.corba.se.spi.legacy.connection.GetEndPointInfoAgainException;
import com.test.customPartition.TestPartition;
import com.test.flatMap_1.SunFunctionStates1;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;

import java.io.IOException;

public class TestStates {
    public static void main(String[] args) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(6000);
//
//		FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///Users/apple/Desktop/rockdata").toUri(),new Path("file:///Users/apple/Desktop/savepoint").toUri());
//
//        env.setStateBackend(new RocksDBStateBackend(fsStateBackend));
//        testMethod1(env);
//		testMethod2(env);
		testMethod3(env);
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
		DataStreamSource<String> input = env.addSource(KafkaUtil.getKafkaConsumer09Source("wordcount"));
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


	// 自定义分组
	public static void testMethod3(StreamExecutionEnvironment env){
		DataStreamSource<Tuple2<Long,Long>> dataStreamSource = env.fromElements(Tuple2.of(1L,3L), Tuple2.of(1L,5L), Tuple2.of(1L,3L), Tuple2.of(1L,7L));
		dataStreamSource.partitionCustom(new TestPartition(),1 ).flatMap(new SunFunctionStates1()).print().setParallelism(1);

    }
}
