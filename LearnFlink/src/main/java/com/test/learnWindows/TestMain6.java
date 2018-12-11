package com.test.learnWindows;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.customAssignTAndW.CustomAssignerTimestampsAndWatermark;
import com.test.customEvictor.CustomEvictor;
import com.test.customTrigger.CustomTrigger;
import com.test.sink.CustomPrintTuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
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
import java.util.Iterator;
import java.util.List;

/**
 * 学习和测试windows 相关的类
 * Window Assigner ：决定某个元素被分配到哪个/哪些窗口中去。
 * Trigger ： 触发器，进行窗口的处理或清除，每个窗口都会拥有一个的Trigger。
 * Evictor ： “驱逐者”，类似filter作用。在Trigger触发之后，window被处理前，EVictor用来处理窗口中无用的元素。
 * TimestampAssigner ：分配字符串 指定系统要处理的字符串 比如是 时间时间，摄取时间，处理时间
 *
 * 窗口学习心得
 * 1： 类的调用关系
 * 首先介绍这个类
 * org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
 * 这个类继承了 AbstractUdfStreamOperator类和实现了OneInputStreamOperator，Triggerable的接口
 * 想知道WindowOperator类的作用必须直到他的父类和所实现的结构的作用
 * AbstractUdfStreamOperator 用户自定义操作的基类
 * 它的最内部实现了StreamOperator的接口而StreamOperator类是流操作员的基础接口 想要对流进行操作必须要实现的基础接口  org.apache.flink.streaming.api.operators.StreamOperator 详情进入类内查看
 *
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator WindowOperator实现的接口
 * 此接口的作用：具有一个输入流的流操作接口 主要接口processElement 上一个任务会通过网络调用这个接口转入上个任务的结果
 *
 * 学习窗口的处理逻辑
 *首先要查看processElement方法
 * 方法中的操作 进入方法查看
 * 看过类里面的注释大概就明白了一个元素到达后的基本处理逻辑
 * 我们以见到的翻滚窗口为例进行分析
 * 调用触发器时：会做一个判断 判断当前窗口的最大时间是否小于等于当前的水印时间 如果小于或者等于 发射此窗口内的数据 否则注册事件时间定时器  和返回结果不fire和不purge
 * 注册事件时间定时器非常重要 理解
 * 问题：窗口时间是靠什么时间推动的 推动的原理是啥
 * 原理： 当注册事件时间定时器时 我们会把包含当前元素的窗口注册为一个定时器到一个优先级队列中org.apache.flink.runtime.state.heap.HeapPriorityQueueSet 提供注册服务的对象是
 * org.apache.flink.streaming.api.operators.HeapInternalTimerService 这个类初始化时 会把WindowOperator对象传进去应为它也实现了Triggerable接口
 *
 *重点：org.apache.flink.streaming.runtime.io.StreamInputProcessor	类 是任务上一个任务的交互类他的前面模块是网络模块就是和上一个任务联系的
 *
 * 这个类中有一个方法processInput非常重要 上一个任务的结果进入的此任务的入口，这个任务对上个任务的结果做操作从这里开始
 * 如果它的前一个任务分配了水印 就会对水印进行操作 回调用StatusWatermarkValve类的inputWatermark方法把当前的水印状态和输入通道转入
 * 如果看调用详情 看org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve类
 * 最后回调用HeapInternalTimerService的advanceWatermark方法 此方法会从堆栈中取出定时器 把定时器的所指定的窗口中的最大值和当前水印进行比较如果小于等于当前水印，调用WindowOperator类的onEventTime方法装入当前定时器此定时被从堆栈中移除
 *  然后就是调用触发器上下文就是WindowOperator的一个内部类Context 其实此方法就会调用触发器的onEventTime方法
 *  此时触发器内窗口的最大值和定时器内窗口的最大值如果相等 就返回触发结果fire 否则继续
 *
 *
 *
 *  第二种EvictingWindowOperator继承自WindowOperator
 *  它主要实现Evictor它的方法 实现对窗口中的一些无用数据的过滤
 *
 *  主要方法emitWindowContents 对发射出去的内容进行处理
 *  会把所用的发射数据和尺寸转入
 *
 *
 *
 *
 *  窗口水印的触发逻辑
 *
 *  每次接受一个数据AssignerWithPeriodicWatermarks分配器就会分配一个当前的时刻的水印 水印就是当前元素的事件时间减去等待时间
 *
 *  一个窗口的大小是一定的 当超出这个窗口的数据过来时 根据这个时间产生的水印就可能大于上一个时间窗口或者等于
 *  当上一个时间窗口的最大值小于或者等于当前水印时 就触发这个窗口
 *
 *  比如 可等待时间设为   2  时间窗口大小喂3秒   发送的数据为1.2.3.4.5.6.7.8.9.10
 *
 *  第一个时间窗口为 开始时间是 1 - （1 - 0 + 3）% 3 = 0
 *  所以一个窗口是 0 ~ 3 不包含3
 *  1  0 ~ 3
 *  2  3 ~ 6
 *  3  6 ~ 9
 *  4  9 ~ 12
 *
 *  index   element   watermark     window     condition
 *  1       1          -1           0 ~ 3       2 <= -1
 *  2       2          0          	0 ~ 3       2 <= 0
 *  3       3          1			3 ~ 6		2 <= 1   5 <= 1
 *  4       4          2			3 ~ 6		2 <= 2   5 <= 2  第一个窗口被触发
 *  5       5          3			3 ~ 6       5 <= 3
 *  6       6          4			6 ~ 9       5 <= 4   8 <= 4
 *  7       7          5			6 ~ 9		5 <= 5   8 <= 5  第二个窗口被触发
 *  8       8          6			6 ~ 9		8 <= 6
 *  9       9          7            9 ~ 12      8 <= 7   11 <= 7
 *  10      10         8            9 ~ 12		8 <= 8   11 <= 8  第三个窗口被触发
 *  11      11         9            9 ~ 12		11 <= 9
 *  上面就是窗口运行逻辑
 */


public class TestMain6 extends AbstractTestMain1 {

	private static JsonParser jsonParser = new JsonParser();
	public static void main(String[] args) {
		DataStreamSource<String> input = null;
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			input =getInput();
//			testMethod1(input);
//			testMethod2(input);
//			testMethod3(input);
//			testMethod4(input);
//			testMethod5(input);
			testMethod7();
//			testMethod8();
//			testMethod10(input);
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

	public static void testMethod7() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.getField(0);
			}
		});

		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply(new WindowFunction<Tuple3<String,Integer,Long>, Tuple4<String,String,Integer,Long>, String, TimeWindow>() {
			@Override
			public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector< Tuple4<String,String,Integer,Long>> out) throws Exception {
				Iterator<Tuple3<String, Integer, Long>> iterator = input.iterator();
				while (iterator.hasNext()){
					Tuple3<String, Integer, Long> tuple3 = iterator.next();
					out.collect(new Tuple4<>(window.toString(),tuple3.getField(0),tuple3.getField(1),tuple3.getField(2)));
				}
			}
		}).setParallelism(3);

		dataStream.addSink(new CustomPrintTuple4()).setParallelism(1);
	}

	public static void testMethod8() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				return value.getField(0);
			}
		});

		DataStream dataStream = keyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply(new AllWindowFunction<Tuple3<String,Integer,Long>, Tuple4<String,String,Integer,Long>, TimeWindow>() {
			@Override
			public void apply(TimeWindow window, Iterable<Tuple3<String, Integer, Long>> values, Collector<Tuple4<String, String, Integer, Long>> out) throws Exception {
				Iterator<Tuple3<String, Integer, Long>> iterator = values.iterator();
				while (iterator.hasNext()){
					Tuple3<String, Integer, Long> tuple3 = iterator.next();
					out.collect(new Tuple4<>(window.toString(),tuple3.getField(0),tuple3.getField(1),tuple3.getField(2)));
				}
			}
		}).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple4()).setParallelism(1);
	}

	public static void testMethod9() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);

		DataStream dataStream = dataStreamSource1.windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).apply(new AllWindowFunction<Tuple3<String,Integer,Long>, Tuple4<String,String,Integer,Long>, TimeWindow>() {
			@Override
			public void apply(TimeWindow window, Iterable<Tuple3<String, Integer, Long>> values, Collector<Tuple4<String, String, Integer, Long>> out) throws Exception {
				Iterator<Tuple3<String, Integer, Long>> iterator = values.iterator();
				while (iterator.hasNext()){
					Tuple3<String, Integer, Long> tuple3 = iterator.next();
					out.collect(new Tuple4<>(window.toString(),tuple3.getField(0),tuple3.getField(1),tuple3.getField(2)));
				}
			}
		});

		dataStream.addSink(new CustomPrintTuple4());
	}

	public static void testMethod10(DataStreamSource<String> input) {
		input.flatMap(new FlatMapFunction<String, TimeAndNumber>() {
			@Override
			public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
				JsonElement jsonElement = jsonParser.parse(value);
				Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
				Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
				out.collect(new TimeAndNumber(timestamp,number));
			}
		}).assignTimestampsAndWatermarks(new CustomAssignerTimestampsAndWatermark()).setParallelism(1).keyBy(new KeySelector<TimeAndNumber, Long>() {
			@Override
			public Long getKey(TimeAndNumber value) throws Exception {
				return value.getNumber();
			}
		}).windowAll(TumblingEventTimeWindows.of(Time.seconds(70),Time.seconds(0))).trigger(new CustomTrigger()).evictor(new CustomEvictor()).reduce(new ReduceFunction<TimeAndNumber>() {
			@Override
			public TimeAndNumber reduce(TimeAndNumber value1, TimeAndNumber value2) throws Exception {
				return new TimeAndNumber(value1.getTimestamp(),value1.getNumber() + value2.getNumber());
			}
		}).setParallelism(2).addSink(new RichSinkFunction<TimeAndNumber>() {
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
