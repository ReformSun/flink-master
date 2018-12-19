package com.test.learnWindows;

import model.Event;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * cep  Complex Event Process  复杂事件处理
 */
public class TestMain11 extends AbstractTestMain11 {

	public static void main(String[] args) {

	}

	public static void testMethod1(){
		List<StreamRecord> inputEvents = getTestMain11data();
		Pattern pattern1 = Pattern.<Event>begin("pattern1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;
			@Override
			public boolean filter(Event value) throws Exception {
				if (value.getA().equals("a")){
					return true;
				}
				return false;
			}
		});

		Pattern pattern2 = pattern1.next("pattern2").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;
			@Override
			public boolean filter(Event value) throws Exception {
				if (value.getA().equals("b")){
					return true;
				}
				return false;
			}
		});

//		NFA nfa = NFACompiler.compile(pattern2, Event.createTypeSerializer(), false);
	}

}
