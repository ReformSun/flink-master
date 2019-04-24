package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3_pr;
import com.test.filesource.FileSourceTuple3;
import com.test.sink.CustomPrintTuple3;
import com.test.util.DataUtil;
import com.test.util.URLUtil;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 学习会话场景的应用场景 在下面的链接中
 * https://yq.aliyun.com/articles/64818
 */
public class TestSessionWindow3 extends AbstractTestMain11{
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
//		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null,null,true);
//		System.out.println(list.size() + " asdfg");
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(10000)).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3_pr<String,Integer,Long>(0,2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});
//		testMethod1(keyedStream);
		testMethod2(keyedStream);

	}

	public static void testMethod1(KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream){
		DataStream dataStream1 = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
			.trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);
		sink(dataStream1);
	}

	public static void testMethod2(KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream){
		DataStream dataStream1 = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
			.trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);
		sink(dataStream1);
	}



	private static void sink(DataStream dataStream){
		dataStream.addSink(new CustomPrintTuple3("TestSessionWindow3")).setParallelism(1);
	}
}
