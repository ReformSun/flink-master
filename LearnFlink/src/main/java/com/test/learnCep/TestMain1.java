package com.test.learnCep;

import com.test.customAbstract.AbstractTestSocket;
import com.test.customAssignTAndW.CustomAssignerTimesTampEvent;
import com.test.util.DataUtil;
import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collection;

import static com.test.learnCep.CommonCep.timeOut;

/**
 * 用户
 */
public class TestMain1 extends AbstractTestSocket {
	public static void main(String[] args) {
		try{
//			env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//			testSocket();
//			testCollection();
//			testMethod5();
//			testMethod6();
//			testMethod7();
			testMethod8();


		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("test11");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void testSocket(){
		DataStream<Event> dataStreamSource1 = getInputFromSocket();
		//			testMethod1_1(dataStreamSource1);
		//		testMethod1_2(dataStreamSource1);
	}

	public static void testCollection(){
		Collection<Event> inputEvents = DataUtil.getEventCollection(4,10,30);
		DataStreamSource<Event> input =  env.fromCollection(inputEvents);
//		testMethod1_1(input);
//		testMethod1_2(input);
//		testMethod1_3(input);
		testMethod4(input);
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
		CommonCep.simplePattern(dataStream,pattern,"first");
	}

	/**
	 * 测试组合条件 And or
	 * 加延迟条件和不加延迟条件
	 */
	public static void testMethod1_2(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
//		timeOut(dataStream,pattern,"second");
//		CommonCep.simplePattern(dataStream,pattern,"first");
		CommonCep.simplePattern(dataStream,pattern,null);
	}

	public static void testMethod1_3(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = CommonMain.getPatternOr();
//		timeOut(dataStream,pattern,"second");
		CommonCep.simplePattern(dataStream,pattern,null);
	}

	/**
	 * 匹配一个流中，一个条件被匹配次数
	 * 至少一次 oneOrMore
	 * 正好4次 times(4)
	 * 匹配2次到3次 times(2, 4)
	 */
	public static void testMethod4(DataStream<Event> dataStream){
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore();
		CommonCep.simplePattern(dataStream,pattern,null);
	}

	/**
	 * until 意味着如果匹配给定条件的事件发生，则不再接受该模式中的事件
	 * 仅适用 oneOrMore方法
	 * 匹配一次到多次
	 * 但是知道事件c等于4.0就结束
	 */
	public static void testMethod5(){
		DataStreamSource<Event> input =  env.fromCollection(DataUtil.getEventCollection(4,10,30));
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore().until(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getC() == 4.0;
			}
		});
		CommonCep.simplePattern(input,pattern,null);
	}

	/**
	 * 下面几个测试中
	 * 严格连续性
	 * 宽松连续性
	 * 非确定宽松连续性
	 * 都可以增加within方法，表示这个匹配在一定时间内
	 * 上面的所有匹配模式中加上not
	 * notnext 表示希望一个事件不紧随着另一种事件出现
	 * notFollowedBy 不希望两个事件之间任何地方出现该事件
	 *
	 * 注意：模式序列不能以notFollowedBy（）结束   NOT模式前面不能有可选模式。
	 *
	 */

	/**
	 * next 对应的是严格连续性
	 * 下面代码代表的业务逻辑是
	 * 第一个事件满足A条件 第二个事件满足B条件 并且在一定得时间内 10秒
	 *  传入事件序列
	 * {a,b,d,f,c}
	 * 匹配模式
	 * {a,f}
	 * 输出事件序列
	 * {}
	 */
	public static void testMethod6(){
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<Event> input =  env.fromCollection(DataUtil.getEventCollection(1,2,30));
		DataStream<Event> dataStream = input.assignTimestampsAndWatermarks(new CustomAssignerTimesTampEvent());
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).next("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 60;
			}
		}).within(Time.seconds(11));
		CommonCep.simplePattern(dataStream,pattern,null);
	}

	/**
	 * followedBy 为宽松连续性
	 * 宽松连续性
	 * 第一个事件满足A条件，直到第二个时间满足B条件 打印两个时间
	 * 中间如果有不匹配B条件的事件丢弃
	 * 比如 传入事件为{a,b,d,f,c}
	 * 匹配模式是{a,f}
	 * 匹配结果 {a,f}
	 *
	 * 如果使用 next匹配
	 * 则匹配不到任何结果
	 *
	 */
	public static void testMethod7(){
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<Event> input =  env.fromCollection(DataUtil.getEventCollection2());
		DataStream<Event> dataStream = input.assignTimestampsAndWatermarks(new CustomAssignerTimesTampEvent());
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).followedBy("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 70;
			}
		});
		//.within(Time.seconds(11))
		CommonCep.simplePattern(dataStream,pattern,null);
	}

	/**
	 * followedByAny 非确定宽松连续性
	 *
	 * 非确定宽松连续性
	 * 传入事件序列
	 * {a,b,d,f,f1,c}
	 * 匹配条件
	 * {a,f}
	 * 输出匹配事件序列
	 * {a,f}  {a,f1}
	 *
	 */
	public static void testMethod8(){
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<Event> input =  env.fromCollection(DataUtil.getEventCollection2());
		DataStream<Event> dataStream = input.assignTimestampsAndWatermarks(new CustomAssignerTimesTampEvent());
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).followedByAny("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 70;
			}
		});
		//.within(Time.seconds(11))
		CommonCep.simplePattern(dataStream,pattern,null);
	}







}
