package com.test.kafkaConnecter;

import com.test.learnState.CustomStreamEnvironment;
import com.test.learnWindows.KafkaUtil;
import com.test.util.FileWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 *
 */
public class TestMain1 {
//	static final CustomStreamEnvironment env = new CustomStreamEnvironment();
static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	static DataStreamSource<String> input = null;
	public static void main(String[] args) {
		env.enableCheckpointing(6000);
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

	/**
	 * {@link org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode}
	 * {@link org.apache.flink.streaming.connectors.kafka.config.StartupMode}
	 *
	 * 当设置 提交kafka偏移量的模式为 当执行完一个checkpoint 和 设置kafka开始模式为从已提交的偏移量开始消费时
	 * 会出现提交偏移量和消费的偏移量有重复数据
	 * 分析因为我设置的checkpoint为6秒钟，所以会出现还没有检查点，但是已经消费，但由于没有开始检查点所有不提交，这
	 * 两种模式结合使用会出现数据计算重复
	 *
	 *
	 * 但是如果设置周期性的提交，默认时间是5秒钟，这种也会出现数据是消费到了，但是在流中执行中某个算子出现问题，没有统计到
	 * 但是这是偏移量已经被提交，会造成数据的丢失
	 *
	 * 通过GROUP_OFFSETS和ON_CHECKPOINTS模式加上安全点启动
	 *
	 * 当三者结合时，可以实现数据的不丢失，重复计算，
	 * 因为当我们使用关闭任务并触发检查点时，检查点完成就会提交偏移量，如果采用指定安全点开始或者直接开始都能实现数据不丢失，重复计算等问题
	 *
	 * 主要考虑任务在非正常情况下的问题
	 *
	 */
	public static void testMethod2(){
		try{
			FlinkKafkaConsumer010 flinkKafkaConsumer010 = KafkaUtil.getKafkaConsumer010Source("123abc");
			input = env.addSource(flinkKafkaConsumer010);

			flinkKafkaConsumer010.setCommitOffsetsOnCheckpoints(true);
			// 从
//			flinkKafkaConsumer010.setStartFromLatest();
//			flinkKafkaConsumer010.setStartFromEarliest();
			flinkKafkaConsumer010.setStartFromGroupOffsets();
			errorTest();
//			sendData();
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

	/**
	 * 当某个算子有异常信息时，不影响任务执行但是会一直发送错误之前的数据
	 */
	private static void errorTest(){
    	input.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				if (value.equals("7300")){
					throw new Exception("测试错误");
				}
				return value;
			}
		}).addSink(new SinkFunction<String>() {
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
