package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.filesource.FileSourceTuple3;
import com.test.util.DataUtil;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;

public class TestWindow3 extends AbstractTestMain11{
	public static void main(String[] args) {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		try{
			common();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void common(){
//		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
//		System.out.println(list.size() + " asdfg");
//		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(10000))
//			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3())
//			.setParallelism(1);
//		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
//			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
//				@Override
//				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
////					FileWriter.writerFile(value,"test1.txt");
//					return value.getField(0);
//				}
//			});
//
//		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
//			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));
//		testMethod1(windowedStream);
	}
}
