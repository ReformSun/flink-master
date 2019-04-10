package com.test.learnCep;

import com.test.customAbstract.AbstractTestCollection;
import com.test.customAssignTAndW.CustomAssignerTimesTampEvent;
import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.test.learnCep.CommonCep.*;

/**
 * 时间延迟
 */
public class TestMain4 extends AbstractTestCollection {

	public static void main(String[] args) {
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//			testMethod1();
			testMethod2();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("test11");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
		timeOut(getDataStream(),pattern,null);
	}

	public static void testMethod2(){
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().within(Time.seconds(2));
		simplePattern(getDataStream(),pattern,null);
	}


	public static DataStream<Event> getDataStream() {
		DataStream<Event> dataStream = getInputFromCollection(3,6,10);
		dataStream.assignTimestampsAndWatermarks(new CustomAssignerTimesTampEvent());
		return dataStream;
	}
}
