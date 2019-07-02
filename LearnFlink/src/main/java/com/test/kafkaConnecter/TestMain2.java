package com.test.kafkaConnecter;

import com.test.learnWindows.KafkaUtil;
import com.test.util.FileWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class TestMain2 {
	static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	static DataStreamSource<String> input = null;
	public static void main(String[] args) {
		env.enableCheckpointing(60000);
//        env.setRestartStrategy();
		FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///Users/apple/Desktop/state/checkpointData").toUri(),new Path
			("file:///Users/apple/Desktop/state/savepointData").toUri());
		env.setStateBackend(new RocksDBStateBackend(fsStateBackend));

//		testMethod1();
		testMethod2();
		start();
	}


	public static void testMethod1(){
		try{
			input = env.addSource(KafkaUtil.getKafkaConsumer010Source("123abc"));
			sendData();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void testMethod2(){
		try{
			FlinkKafkaConsumer010 flinkKafkaConsumer010 = KafkaUtil.getKafkaConsumer010Source("123abc");
			input = env.addSource(flinkKafkaConsumer010);

			flinkKafkaConsumer010.setCommitOffsetsOnCheckpoints(true);
			// 从
//			flinkKafkaConsumer010.setStartFromLatest();
//			flinkKafkaConsumer010.setStartFromEarliest();
			flinkKafkaConsumer010.setStartFromGroupOffsets();
			sendData();
		}catch (Exception e){
			e.printStackTrace();
		}
	}


	private static void sendData(){
		input.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				FileWriter.writerFile(value,"test.txt");
			}
		});
	}


	private static void start(){
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
