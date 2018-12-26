package com.test.learnCep;

import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class CommonMain {
	/**
	 * 两个where相当于And 并
	 * @param
	 */
	public static Pattern<Event,Event> getPatternWhere(){
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		return pattern;
	}

	public static Pattern<Event,Event> getPatternWhere1(){
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		});
		return pattern;
	}

	/**
	 * 一个where和or相当于or 或
	 * @param
	 */
	public static Pattern<Event,Event> getPatternOr(){
		Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).or(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		return pattern;
	}
}
