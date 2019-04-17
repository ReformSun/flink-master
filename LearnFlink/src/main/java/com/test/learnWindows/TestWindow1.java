package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomPrintTuple4;
import com.test.util.DataUtil;
import com.test.util.FileWriter;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import model.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

/**
 * 默认的时间时间触发器{@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}
 *
 * 窗口被触发的条件
 *  {@link org.apache.flink.streaming.api.operators.HeapInternalTimerService}类的advanceWatermark方法
 *
 * 如果时间时间小于或者等于水位时间取出注册的定时器，定时器中包含这个事件的数据信息
 * 如果满足条件会调用org.apache.flink.streaming.runtime.operators.windowing.WindowOperator的onEventTime方法
 * 然后会再调用
 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.WindowContext}类的onEventTime方法
 * 这之前已经把定时器内包含的key值。赋值给了上下文。这个是如果去表中去取数据就能取到事件的信息
 * (逻辑详情查看{@link org.apache.flink.runtime.state.heap.HeapValueState}的测试方法)
 * 然后最后会调用自定义的或者默认的时间触发器{@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}的onEventTime方法
 * 判断时间时间是否等于窗口的最大时间如果等于发送数据
 *
 * 还有另外一个处理逻辑：
 * 数据过来时调用 {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}类的processElement方法
 * 会调用{@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}的onElement方法
 * 这个方法也有一个判断条件，窗口最大值小于或者等于当前水位
 *
 * 详细学习可以进入{@link org.apache.flink.streaming.runtime.operators.windowing}所在包的测试文件
 * 中的LearnWindowOperatorTest方法
 * 如果还想学的更清晰可以从{@link org.apache.flink.streaming.runtime.io.StreamInputProcessor}类的
 * processInput方法开始学 这个是输入过来的入口
 */
public class TestWindow1 extends AbstractTestMain11{
	public static void main(String[] args) {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		try{
//			testMethod2();
			testMethod3();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1() {
		DataStreamSource<Event> dataStreamSource = env.fromCollection(getTestMain11data());
		dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
			@Override
			public long extractAscendingTimestamp(Event element) {
				return element.getTime();
			}
		})
			.keyBy("a")
			.window(TumblingEventTimeWindows.of(Time.minutes(1),Time.milliseconds(100)))
			.apply(new WindowFunction<Event, String, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple tuple, TimeWindow window, Iterable<Event> input, Collector<String> out) throws Exception {

				}
			});
	}


	public static void testMethod2(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getTuple3_Int_timetamp_OrderedTime_1M(5);
		System.out.println(list.size() + " asdfg");
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3())
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
			@Override
			public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
				FileWriter.writerFile(value,"test1.txt");
				return value.getField(0);
			}
		});

		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0))).apply(new WindowFunction<Tuple3<String,Integer,Long>,
			Tuple4<String,String,Integer,Long>, String, TimeWindow>() {
			private int count = 0;
			@Override
			public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector< Tuple4<String,String,Integer,Long>> out) throws Exception {
				Iterator<Tuple3<String, Integer, Long>> iterator = input.iterator();
				String threadname = Thread.currentThread().getName();
				while (iterator.hasNext()){
					Tuple3<String, Integer, Long> tuple3 = iterator.next();
					out.collect(new Tuple4<>(threadname + count,tuple3.getField(0),tuple3.getField(1),tuple3.getField(2)));
				}
				count ++;
			}
		}).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple4()).setParallelism(1);
	}

	/**
	 * (a,1,1534472000000)
	 * (a,1,1534472000192)
	 * (a,2,1534472040138)
	 * (a,3,1534472002365)
	 * (a,1,1534472060000)
	 * 统计结果
	 * (a,2,1534472000000)
	 * (a,3,1534472040138)
	 *
	 * (a,1,1534472000000)
	 * (a,1,1534472025860)
	 * (a,2,1534472011500)
	 * (a,3,1534472045604)
	 * (a,1,1534472060000)
	 * 统计结果
	 * (a,4,1534472000000)
	 * (a,4,1534472045604)
	 */
	public static void testMethod3(){
//		List<Tuple3<String,Integer,Long>> list = DataUtil.getTuple3_Int_timetamp_timeOutOfOrder_1M(5);
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		System.out.println(list.size() + " asdfg");
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3())
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});

		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0))).trigger(new EventTimeTrigger()).sum(1).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

}
