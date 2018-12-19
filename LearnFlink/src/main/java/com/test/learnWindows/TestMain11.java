package com.test.learnWindows;

import com.test.sink.CustomPrintEvent;
import model.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * cep  Complex Event Process  复杂事件处理
 */
public class TestMain11 extends AbstractTestMain11 {

	public static void main(String[] args) {
		try{
			testMethod1();
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
		List<Event> inputEvents = getTestMain11data2();
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

		DataStream<Event> result =  CEP.pattern(dataStreamSource1,pattern1).select(new PatternSelectFunction<Event,Event>() {

			@Override
			public Event select(Map<String, List<Event>> pattern) throws Exception {
				System.out.println("dd");
				return new Event(1,"d",2.0);
			}
		}).setParallelism(1);

		result.addSink(new CustomPrintEvent()).setParallelism(1);
	}


}
