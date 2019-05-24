package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3_pr;
import com.test.filesource.FileSourceTuple3;
import com.test.sink.CustomPrintTuple3;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestWindow3 extends AbstractTestMain11{
	public static void main(String[] args) {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		try{
			testMethod1();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(200)).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3_pr<String,Integer,Long>(0,2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
					return value.getField(0);
				}
			});

		DataStream dataStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)))
			.trigger(new EventTimeTrigger())
			.sum(1).setParallelism(1);

		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}
}
