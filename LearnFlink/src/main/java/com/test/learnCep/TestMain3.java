package com.test.learnCep;

import com.test.customAbstract.AbstractTestCollection;
import com.test.util.FileWriter;
import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.util.Iterator;

import static com.test.learnCep.CommonCep.simplePattern;

/**
 * 学习condition
 * 迭代条件
 * 简单条件
 * 组合条件
 * 停止条件
 * 连续事件条件 （1.严格连续性 2.宽松连续性 3.非确定性宽松连续性）
 */
public class TestMain3 extends AbstractTestCollection {
	public static void main(String[] args) {
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//			testMethod1();
//			testMethod2();
//			testMethod3();
//			testMethod4();
//			testMethod4_1();
//			testMethod5();
//			testMethod6();
//			testMethod7();
//			testMethod7_1();
//			testMethod8();
//			testMethod8_1();
//			testMethod9();
//			testMethod9_1();
//			testMethod9_2();
//			testMethod9_3();
//			testMethod9_4();
//			testMethod9_5();
			testMethod10();
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
	 * 迭代条件
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * getEventsForPattern 的数据
	 *  没有数据
	 *  1  2  3  4  5  6  ...
	 *  输出数据
	 *  1  2  3  4  5  6  ...
	 */
	public static void testMethod1() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
		commonMethod(pattern);
	}
	/**
	 * 迭代条件
	 * 加上oneOrMore后
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * getEventsForPattern 的数据
	 *  0  1  1  0  12  12   2    2   0 123  123  23    23  3  3    0    1234    1234  234 234   34 34 4 4 0 ....
	 *  1  2  2  2  3    3   3    3   3 4     4   4      4  4  4    4     5       5     5   5     5  5
	 *  输出数据
	 *  1 12 2 123 23  3   1234 234 34  4  12345 2345 345 45 5 123456 23456    3456 456   56    6 ......
	 */
	public static void testMethod2() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
		commonMethod(pattern.oneOrMore());
	}


	/**
	 * 停止条件
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * 1 12 2 123 23 3 5 56 6 567 67 7 5678 678 78 8
	 * 在3的位置发生了停止，然后从5的位置重新开始
	 */
	public static void testMethod3() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore().until(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getC() == 3.0D;
			}
		});
		simplePattern(getInputFromCollection(15,19,20),pattern,"first");

	}
	/**
	 * 强制连续性
	 * 3,6,20,9
	 * 1 12 2 123 23 3 7 78 8 789 89 9
	 */
	public static void testMethod4() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore().consecutive();
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"first");
	}

	/**
	 *  Combinations not applicable to org.apache.flink.cep.pattern.Quantifier@e01fc24a!
	 */
	public static void testMethod4_1() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().consecutive();
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"first");
	}

	/**
	 * 宽松的连续性
	 * 3,6,20,9
	 *  1 12 2 123 23 3 1237 237 37 7 12378 2378 378 78 8 123789 23789 3789 789 89 9
	 */
	public static void testMethod5() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore();
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"first");
	}

	/**
	 * 非确定性宽松连续性
	 * 3,6,20,9
	 * 1 12 2 123 13 23 3 1237 127 137 17 237 27 37 7 12378 1238 1278 128 1378 138 178 18 2378 238 278 28 378 38 78 8 ..,
	 */
	public static void testMethod6() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().oneOrMore().allowCombinations();
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"first");
	}



	/**
	 * 宽容连续性
	 * 3,6,20,9
	 * first 1 2 3 7 8 9
	 * end 2 3 4 8 9 10
	 */
	public static void testMethod7() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return true;
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,null);
	}

	/**
	 * 2 3 7 7 7 7 8 9
	 */
	public static void testMethod7_1() {
		Pattern<Event,Event> pattern = Pattern.<Event>begin("start").followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}

	/**
	 * 非确定性的宽松连续性
	 * 3,6,20,9
	 * first 1 1 2 1 2 3 1 2 3 1 2 3 1 2 3 1 2 3 7 1 2 3 7 8 1   2  3   7    8  9   1 2 3 7 8 9 1 2 3 7 8 9 1 2 3 7 8 9
	 * end   2 3 3 4 4 4 5 5 5 6 6 6 7 7 7 8 8 8 8 9 9 9 9 9 10 10  10  10   10 10 ....都是6个
	 */
	public static void testMethod8() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().followedByAny("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return true;
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,null);
	}

	/**
	 * 2 3 3 7 7 7 7 7 7 8 8 8 8 8 8 8 9 9 9 9 9 9 9 9
	 */
	public static void testMethod8_1() {
		Pattern<Event,Event> pattern = Pattern.<Event>begin("start").followedByAny("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}

	/**
	 * 严格连续性
	 * first 1 2 3 7 8 9
	 * end 2 3 4 8 9 10
	 */
	public static void testMethod9() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().next("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return true;
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,null);
	}

	/**
	 * 2 3 7 8 9
	 */
	public static void testMethod9_1() {
		Pattern<Event,Event> pattern = Pattern.<Event>begin("start").next("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		});
		simplePattern(getInputFromCollection( 3,6,20,9),pattern,"end");
	}

	/**
	 * 2 3 4 8 9 10
	 */
	public static void testMethod9_2() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().next("end");
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}

	/**
	 * 1 2 3 7 8 9
	 */
	public static void testMethod9_3() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere();
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"first");
	}

	/**
	 * 2 3 4 8 9 10
	 */
	public static void testMethod9_4() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere1().next("end");
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}

	/**
	 * 2 3 8 9
	 */
	public static void testMethod9_5() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere1().next("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}

	public static void testMethod10() {
		Pattern<Event,Event> pattern = CommonMain.getPatternWhere().notNext("end");
		simplePattern(getInputFromCollection(3,6,20,9),pattern,"end");
	}


	public static void commonMethod(Pattern<Event,Event> pattern) {
		Pattern<Event,Event> pattern2 = pattern.where(new IterativeCondition<Event>() {
			int numberofTimes = 0;
			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				numberofTimes ++;
				Iterator<Event> iterator = ctx.getEventsForPattern("first").iterator();
				while (iterator.hasNext()){
					Event event = iterator.next();
					event.setNumberOfTimes(numberofTimes);
					FileWriter.writerFile(event.toString(),"test1.txt");
				}

				value.setNumberOfTimes(numberofTimes);
				FileWriter.writerFile(value.toString(),"test2.txt");
				return true;
			}
		});
		simplePattern(getInputFromCollection(6,9,10),pattern2,"first");
	}


}
