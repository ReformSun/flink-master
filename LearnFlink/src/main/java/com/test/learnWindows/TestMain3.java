package com.test.learnWindows;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;
import test.TimeAndNumber;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestMain3 {
	private static JsonParser jsonParser = new JsonParser();
	public static void main(String[] args) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
			env.getConfig().disableSysoutLogging();
			env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
			env.enableCheckpointing(5000);
			FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///root/rockdata").toUri(),new Path("file:///root/savepoint").toUri());
			env.setStateBackend(new RocksDBStateBackend(fsStateBackend));
			input = env.addSource(KafkaUtil.getKafkaConsumer09Source("ddddddd")).setParallelism(1);
//			testMethod1(input);
			testMethod2(input);
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, SunWordWithCount>() {
			@Override
			public void flatMap(String value, Collector<SunWordWithCount> out) throws Exception {
				out.collect(new SunWordWithCount(value,1));
			}
		}).setParallelism(2).keyBy("word").reduce(new RichReduceFunction<SunWordWithCount>() {
			@Override
			public SunWordWithCount reduce(SunWordWithCount value1, SunWordWithCount value2) throws Exception {
				return new SunWordWithCount(value1.word,value1.count + value2.count);
			}
		}).setParallelism(4).addSink(new CustomWordCountPrint()).setParallelism(1);
	}

	/**
	 * 测试目的 flink的任务通过触发检查点并通过检查点进行启动数据消费是否会丢失、
	 *
	 *
	 *测试结果 数据未丢失
	 *
	 * 但是出现了一种现象
	 *  发送数据中有时间戳 时间是连续的
	 *  但是通过savepoint启动时 时间变的不连续 但是数据不会丢失 不连续的数据过一会才会出现
	 *
	 *  可能原因分析 sink的平行度为2造成的
	 *
	 *  测试结果 猜测正确
	 *
	 * @param input
	 */
	public static void testMethod2(DataStreamSource<String> input) {

		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {

			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).setParallelism(1).addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				java.nio.file.Path logFile = Paths.get("/root/test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}
}
