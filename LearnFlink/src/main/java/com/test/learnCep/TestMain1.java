package com.test.learnCep;

import com.test.customAbstract.AbstractTestSocket;
import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import static com.test.learnCep.CommonCep.timeOut;

/**
 * 用户
 */
public class TestMain1 extends AbstractTestSocket {
	public static void main(String[] args) {
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
			DataStream<Event> dataStreamSource1 = getInputFromSocket();

			testMethod1_1(dataStreamSource1);
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("test11");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 匹配where和flollowedBy
	 * @param dataStream
	 */
	public static void testMethod1_1(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).followedBy("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		timeOut(dataStream,pattern,"second");
//		testMethod2(dataStream,pattern);
	}

	public static void testMethod1_2(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
		timeOut(dataStream,pattern,"second");
//		testMethod2(dataStream,pattern);
	}

	public static void testMethod1_3(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = CommonMain.getPatternOr();
		timeOut(dataStream,pattern,"second");
//		testMethod2(dataStream,pattern);
	}

}
