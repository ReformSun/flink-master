package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomPrintTuple4;
import com.test.util.DataUtil;
import com.test.util.FileWriter;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

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
					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});

		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0))).sum(1).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

}
