package com.test.learnWindows;

import com.test.sink.CustomPrintEvent;
import model.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.*;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * http://blog.chinaunix.net/uid-29038263-id-5765739.html
 *
 * cep  Complex Event Process  复杂事件处理
 * 实现原理学习
 * org.apache.flink.cep.nfa.State
 * org.apache.flink.cep.nfa.NFA
 *
 * State代表了一个NFA的状态
 * NFA CEP的具体执行者
 *
 * 每一个key值对应一个NFA 还没有理解这句话的意思
 *
 * 具体学习一下State
 * State包含的属性有
 *  一个匹配规则
 *  Pattern pattern1 = Pattern.<Event>begin("pattern1").where(new SimpleCondition<Event>() {
 *     @Override
*      public boolean filter(Event value) throws Exception {
*           if (value.getA().equals("a")){
*                return true;
*            }
*            return false;
*      }
*    });
 * name 相当与 pattern1
 * stateType 状态类型有四种 Start,Final,Normal,Stop 上面代码对应的是 start状态
 * stateTransitions 代表了从一个状态到另一个状态的转换
 *
 * stateTransitions详情
 * 包含属性为
 * StateTransitionAction action; 表示状态转换的行动 行动分为三个行为TAKE（获取当前事件被把它分配给当前状态），IGNORE（忽略当前事件）和PROCEED（执行状态转换并保留当前事件以进行进一步处理）
 * State<T> sourceState; 它的上一个状态
 * State<T> targetState; 下一个状态
 * IterativeCondition<T> condition; 用户自定义的判断条件，相当与上面代码的SimpleCondition实例化对象 最后会调用这个类的自定义规则
 */
public class TestMain11 extends AbstractTestMain11 {

	public static void main(String[] args) {
		try{
			testMethod();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("test11");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod() {
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		List<Event> inputEvents = getTestMain11data();
		DataStream<Event> dataStreamSource1 = env.fromCollection(inputEvents).setParallelism(1);

		Pattern pattern1 = Pattern.<Event>begin("pattern1").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				if (value.getA().equals("a")){
					return true;
				}
				return false;
			}
		});
//		testMethod1(dataStreamSource1,pattern1);
		testMethod2(dataStreamSource1,pattern1);
	}


	/**
	 * org.apache.flink.cep.operator.SelectCepOperator
	 * org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator
	 *
	 * flink的cep其实基础是和普通流处理是一样的，主要通过SelectCepOperator类实现
	 * 只能一个对一的关系
	 */
	public static void testMethod1(DataStream<Event> dataStream,Pattern<Event,Event> pattern){
		DataStream<Event> result =  CEP.pattern(dataStream,pattern).select(new PatternSelectFunction<Event,Event>() {
			@Override
			public Event select(Map<String, List<Event>> pattern) throws Exception {
				System.out.println("dd");
				List<Event> list = pattern.get("pattern1");

				return list.size() != 0 ? list.get(0) : null;
			}
		}).setParallelism(1);

		result.addSink(new CustomPrintEvent()).setParallelism(1);
	}

	/**
	 * org.apache.flink.cep.operator.FlatSelectCepOperator
	 * @param dataStream
	 * @param pattern
	 */
	public static void testMethod2(DataStream<Event> dataStream,Pattern<Event,Event> pattern) {
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern);
		DataStream<Event> dataStream1 = patternStream.flatSelect(new PatternFlatSelectFunction<Event, Event>() {
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
				System.out.println(pattern.toString());
				List<Event> list = pattern.get("pattern1");
				Iterator<Event> iterator  = (Iterator<Event>) list.iterator();
				while (iterator.hasNext()){
					Event event = iterator.next();
					out.collect(event);
				}
			}
		});
		dataStream1.addSink(new CustomPrintEvent()).setParallelism(1);
	}

	/**
	 * 处理延迟方面的匹配
	 * org.apache.flink.cep.operator.FlatSelectTimeoutCepOperator
	 * @param dataStream
	 * @param pattern
	 */
	public static void testMethod3(DataStream<Event> dataStream,Pattern<Event,Event> pattern) {
		PatternStream<Event> patternStream = CEP.pattern(dataStream,pattern);
		OutputTag<Event> outputTag = new OutputTag<Event>("side-output"){};
		SingleOutputStreamOperator<Event> dataStream1 = patternStream.flatSelect(outputTag, new PatternFlatTimeoutFunction<Event, Event>() {
			@Override
			public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {

			}
		}, new PatternFlatSelectFunction<Event, Event>() {
			@Override
			public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {

			}
		});

		DataStream<Event> dataStream2 = dataStream1.getSideOutput(outputTag);
		dataStream2.addSink(new CustomPrintEvent()).setParallelism(1);


	}


}
