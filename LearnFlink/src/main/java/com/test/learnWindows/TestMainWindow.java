package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTuple;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple2;
import com.test.keyby.KeySelectorTuple;
import com.test.sink.CustomPrint;
import com.test.sink.CustomPrintTuple;
import com.test.sink.CustomPrintTuple2;
import com.test.util.DataUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class TestMainWindow {
	private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private static int i;
	public static void main(String[] args) {
//		testMethod1();
//		testMethod2();
		testMethod3();
		try {
			env.execute("测试");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 测试得出WindowFunction接口中apply方法中的Iterable容器中放入的都是此窗口内的所有数据
	 */
	public static void testMethod1(){
		DataStreamSource<Tuple2<Long,String>> dataStream = env.fromCollection(DataUtil.getTuple2());
		dataStream
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple2(0))
			.keyBy(new KeySelectorTuple(1)).window(TumblingEventTimeWindows.of(Time.minutes(1)))
			.apply(new WindowFunction<Tuple2<Long,String>, String, String, TimeWindow>() {
				@Override
				public void apply(String s, TimeWindow window, Iterable<Tuple2<Long, String>> input, Collector<String> out) throws Exception {
					Iterator<Tuple2<Long,String>> iterator = input.iterator();
					i ++;
					while (iterator.hasNext()){
						System.out.println("aaaa" + i + "d" + iterator.next().toString());
					}
					out.collect("dddddd");
				}
			}).addSink(new CustomPrint("test.txt"));
	}

	public static void testMethod2(){
		DataStreamSource<Tuple2<Long,String>> dataStream = env.fromCollection(DataUtil.getTuple2());
		WindowedStream dataStream1 = dataStream
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple2(0))
			.keyBy(new KeySelectorTuple(1)).window(TumblingEventTimeWindows.of(Time.minutes(3)));
	}
	/**
	 * reduce接口
	 * 实现窗口内的数据成对出现
	 * 加入窗口内的数据为
	 * (1534472120000,aa) (1534472180000,aa) (1534472240000,aa) (1534472300000,aa)
	 * 最后一个输入会触发窗口，在第一个窗口的数据为前三个
	 * 他们的调用
	 * 第一次
	 * 1aaaa1d(1534472120000,aa) 2aaaa1d(1534472180000,aa)
	 * 第二次
	 * 1aaaa2d(1534472120000,aaaa) 2aaaa2d(1534472240000,aa)
	 * 发送完第二次，下一个数据为触发数据
	 * 那么这个窗口内的数据就会直接发送给下一个流节点
	 *
	 */
	public static void testMethod2_1(WindowedStream dataStream){
		DataStream<Tuple2<Long, String>> dataStream1 = dataStream.reduce(new ReduceFunction<Tuple2<Long, String>>() {
			private int i = 0;
			@Override
			public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
				i ++;
				System.out.println("1aaaa" + i + "d" + value1.toString());
				System.out.println("2aaaa" + i + "d" + value2.toString());
				return new Tuple2<>(value1.f0,value1.f1 + value2.f1);
			}
		});
		dataStream1.addSink(new CustomPrintTuple2<Long,String>("test.txt"));
	}


	public static void testMethod3(){
		DataStreamSource<Tuple3<Long,Integer,String>> dataStream = env.fromCollection(DataUtil.getTuple3_Int());
		WindowedStream dataStream1 = dataStream
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTuple(0))
			.keyBy(new KeySelectorTuple<Tuple3<Long,Integer,String>,String>(2)).window(TumblingEventTimeWindows.of(Time.minutes(3)));
		testMethod3_1(dataStream1);
	}

	public static void testMethod3_1(WindowedStream dataStream){
		DataStream<Tuple3<Long,Integer,String>> dataStream1 = dataStream.aggregate(new AggregateFunction<Tuple3<Long,Integer,String>, AverageAccumulator, Double>() {
			@Override
			public AverageAccumulator createAccumulator() {
				return new AverageAccumulator();
			}

			@Override
			public AverageAccumulator add(Tuple3<Long, Integer, String> value, AverageAccumulator accumulator) {
				accumulator.sum += (Integer) value.getField(1);
				accumulator.count ++;
				return null;
			}

			@Override
			public Double getResult(AverageAccumulator accumulator) {
				return accumulator.sum / (double) accumulator.count;
			}

			@Override
			public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
				a.count += b.count;
          		a.sum += b.sum;
          		return a;
			}
		});
		dataStream1.addSink(new CustomPrintTuple("test.txt"));
	}

	static class AverageAccumulator {
       long count;
       Integer sum;
     }
}
