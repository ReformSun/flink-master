package com.test.learnWindows;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.customAssignTAndW.CustomAssignerTimestampsAndWatermark;
import com.test.customEvictor.CustomEvictor;
import com.test.customTrigger.CustomTrigger;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import test.TimeAndNumber;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 学习和测试windows 相关的类
 * Window Assigner ：决定某个元素被分配到哪个/哪些窗口中去。
 * Trigger ： 触发器，进行窗口的处理或清除，每个窗口都会拥有一个的Trigger。
 * Evictor ： “驱逐者”，类似filter作用。在Trigger触发之后，window被处理前，EVictor用来处理窗口中无用的元素。
 * TimestampAssigner ：分配字符串 指定系统要处理的字符串 比如是 时间时间，摄取时间，处理时间
 */
public class TestMain6 {

	private static JsonParser jsonParser = new JsonParser();
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input = null;
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
	public static void testMethod3(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).setParallelism(1).windowAll(TumblingEventTimeWindows.of(Time.seconds(6),Time.seconds(0))).trigger(new Trigger<TimeAndNumber, TimeWindow>() {
			@Override
			public TriggerResult onElement(TimeAndNumber element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
				ctx.registerEventTimeTimer(111);
				System.out.println(ctx.getCurrentWatermark());
				return TriggerResult.FIRE;
			}

			@Override
			public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
				return TriggerResult.FIRE;
			}

			@Override
			public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
				return TriggerResult.FIRE;
			}

			@Override
			public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

			}
		}).reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return null;
			}
		}).setParallelism(1).addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}

	public static void testMethod4(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark()).setParallelism(1).setParallelism(1).windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return new TimeAndNumber(value1.getTimestamp(),value1.getNumber() + value2.getNumber());
			}
		}).addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}

	public static void testMethod5(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark()).setParallelism(1).setParallelism(1).windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).trigger(new CustomTrigger()).reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return new TimeAndNumber(value1.getTimestamp(),value1.getNumber() + value2.getNumber());
			}
		}).addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}

	public static void testMethod6(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark()).setParallelism(1).setParallelism(1).windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).trigger(new CustomTrigger()).evictor(new CustomEvictor()).reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return new TimeAndNumber(value1.getTimestamp(),value1.getNumber() + value2.getNumber());
			}
		}).addSink(new RichSinkFunction<TimeAndNumber>() {
			@Override
			public void invoke(TimeAndNumber value) throws Exception {
				Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		}).setParallelism(1);
	}
}
