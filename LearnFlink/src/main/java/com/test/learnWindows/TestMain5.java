package com.test.learnWindows;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.customAssignTAndW.CustomAssignerTimestampsAndWatermark;
import com.test.customEvictor.CustomEvictor;
import com.test.customTrigger.CustomTrigger;
import com.test.sink.CustomPrint;
import com.test.sink.CustomWordCountPrint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import socket.SocketWindowWordCount;
import test.SunWordWithCount;
import test.TimeAndNumber;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * One-to-One Streams（例如source和map()之间）维护着分区和元素的顺序。这意味着map操作看到的元素个数和顺序跟source操作看到的元素个数和顺序是相同的。
 * Redistributing Streams（例如map()和keyBy、Window之间，还有keyBy、Window和sink之间）的分区发生改变。每个operator subtask
 * 把数据发送到不同的目标subtask上，其发送的依据是选择何种的transformation。例如keyBy操作（基于Hash重新分区），broadcast()或者
 * rebalance() (随机重新分区)。在一个redistributing 交换中，元素之间的顺序仅仅在每一个发送-接受task对中才会被维持。
 *
 * 重分区模式 broadcast广播模式和rebalance随机分区模式
 *
 *
 * @param
 */
public class TestMain5 {
	private static JsonParser jsonParser = new JsonParser();
	public static void main(String[] args) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
//			env.getConfig().disableSysoutLogging();
//			env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//			env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//			env.setParallelism(2);
			input = env.addSource(KafkaUtil.getKafkaConsumer09Source("ddddddd")).setParallelism(1);
//			testMethod1(input);
//			testMethod2(input);
//			testMethod3(input);
//			testMethod4(input);
//			testMethod5(input);
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


	public static void testMethod2(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).setParallelism(1).broadcast().addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				java.nio.file.Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(2);
	}


}
