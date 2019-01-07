package com.test.learnCep;

import com.test.customAbstract.AbstractTestCollection;
import com.test.util.FileWriter;
import model.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;

import static com.test.learnCep.CommonCep.simplePattern;

/**
 *
 * https://github.com/crestofwave1/oneFlink/blob/master/doc/CEP/FlinkCEPOfficeWeb.md
 * 循环模式的学习 quantifiers
 *
 * 这里的测试times,oneOrMore,timesOrMore非常好理解
 *
 * 对于optional和greedy 还没有完全理解
 *
 * greedy 可以使循环模式变的贪婪
 * optional 可以使模式变得可选
 * 比如：times（4） 期待发生四次
 *       times（4）.optional() 期待发生0次或者4次
 */
public class TestMain2 extends AbstractTestCollection {
	public static void main(String[] args) {
		try{
			env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//			testMethod2();
//			testMethod2_1();
//			testMethod2_2();
//			testMethod3();
//			testMethod4();
//			testMethod5();
//			testMethod6();
//			testMethod7();
//			testMethod8();
//			testMethod9();
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

	public static void testMethod1() {
		Pattern<Event,Event> pattern1 = CommonMain.getPatternOr();
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();

		pattern1.oneOrMore().until(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return false;
			}
		});
	}

	/**
	 * 学习循环模式
	 * 期待发生4次
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * a = 3
	 * a = 4  1234
	 * a = 5  1234 2345
	 * a = 6  1234 2345 3456
	 */
	public static void testMethod2() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(4);
		simplePattern(getInputFromCollection(5,9,10),pattern3,"first");
	}

	/**
	 * 学习贪婪模式
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * a = 3
	 * a = 4  1234
	 * a = 5  1234 2345
	 * a = 6  1234 2345 3456
	 *
	 */
	public static void testMethod2_1() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(4).greedy();
		simplePattern(getInputFromCollection(6,9,10),pattern3,"first");
	}

	/**
	 * 学习可选择的模式
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * a = 3
	 * a = 4  1234
	 * a = 5  1234 2345
	 * a = 6  1234 2345 3456
	 *
	 */
	public static void testMethod2_2() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(4).optional();
		simplePattern(getInputFromCollection(0,9,10),pattern3,"first");
	}
	/**
	 * 学习贪婪模式加可选择模式
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * a = 0
	 * a = 3
	 * a = 4  1234
	 * a = 5  1234 2345
	 * a = 6  1234 2345 3456
	 *
	 */
	public static void testMethod2_3() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(4).optional().greedy();
		simplePattern(getInputFromCollection(0,9,10),pattern3,"first");
	}



	/**
	 * 学习循环模式
	 * 期待0或者4四次
	 *
	 * test             a         b             num
	 *  1               0         6             10
	 *  2               3         6             10
	 *  3               4         6             10
	 *  4               5         9             10
	 *  5               8         9             10
	 *  测试1 没有任何数据被打印
	 *  测试2 没有任何数据被打印
	 *  测试3 有四条数据被打印
	 *  测试4 有八条数据被打印 其中有3条重复数据 第二次打印数据的前三条和第一次打印的前三条重复
	 *  测试5 有20条数据被打印 分为5次每次4条被打印 每一次打印的数据的前三条都和前一次的后三条数据相等
	 */
	public static void testMethod3() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(4).optional();
		simplePattern(getInputFromCollection(8,6,10),pattern3,"first");
	}
	/**
	 * 学习循环模式
	 * 期待2,3或者4四次
	 *
	 * test             a         b             num
	 *  1               2         6             10
	 *  2               3         6             10
	 *  3               4         6             10
	 *  4               5         9             10
	 *  5               8         9             10
	 *  测试1 有两条数据被打印
	 *  测试2 没有任何数据被打印
	 *  测试3 有七条数据被打印 第一次打印两条 第二次打印三条 第三次打印两条
	 *  测试4 有八条数据被打印 其中有3条重复数据 第二次打印数据的前三条和第一次打印的前三条重复
	 *  测试5 有20条数据被打印 分为5次每次4条被打印 每一次打印的数据的前三条都和前一次的后三条数据相等
	 */
	public static void testMethod4() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(2,4);
		simplePattern(getInputFromCollection(3,6,10),pattern3,"first");
	}


	/**
	 * 学习循环模式
	 * 期待2,3或者4四次
	 *
	 * test             a         b             num
	 *  1               2         6             10
	 *  2               3         6             10
	 *  3               4         6             10
	 *  4               5         9             10
	 *  5               8         9             10
	 *  测试1 有两条数据被打印
	 *  测试2 没有任何数据被打印
	 *  测试3 有七条数据被打印 第一次打印两条 第二次打印三条 第三次打印两条
	 *  测试4 有八条数据被打印 其中有3条重复数据 第二次打印数据的前三条和第一次打印的前三条重复
	 *  测试5 有20条数据被打印 分为5次每次4条被打印 每一次打印的数据的前三条都和前一次的后三条数据相等
	 */
	public static void testMethod5() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(2,4).greedy();
		simplePattern(getInputFromCollection(3,6,10),pattern3,"first");
	}

	public static void testMethod6() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(2,4).optional();
		simplePattern(getInputFromCollection(3,6,10),pattern3,"first");
	}

	public static void testMethod7() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.times(2,4).optional().greedy();
		simplePattern(getInputFromCollection(3,6,10),pattern3,"first");
	}

	/**
	 * 期待1次或者更多次的发生
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * 1 12 2 123 23 3 1234 234 34 4 12345 2345 345 45 5 123456 23456 3456 456 56 6 1234567 234567 34567 4567 567 67 7 .....
	 */
	public static void testMethod8() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.oneOrMore();
		simplePattern(getInputFromCollection(8,9,10),pattern3,"first");
	}

	/**
	 * 期待2次或者坑多次的发生
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * 12 123 23 1234 234 34 12345 2345 345 45 123456 23456 3456 456 56 1234567 234567 34567 4567 567 67 .....
	 */
	public static void testMethod9() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere();
		Pattern<Event,Event> pattern3 = pattern2.timesOrMore(2);
		simplePattern(getInputFromCollection(8,9,10),pattern3,"first");
	}
	/**
	 * 期待2次或者坑多次的发生
	 * 加入数据是1 2 3 4 5 6 7 8 9
	 * SimpleCondition value
	 * 1 3 5 7 9 11
	 * 1 2 2 2 3 3 3 3 3 4 4 4 4 4 4 4 5 5 5 5 5 5 5 5 5
	 * 12 123 23 1234 234 34 12345 2345 345 45 123456 23456 3456 456 56 1234567 234567 34567 4567 567 67 .....
	 */
	public static void testMethod10() {
		Pattern<Event,Event> pattern2 = CommonMain.getPatternWhere().oneOrMore();
		commonMethod(pattern2);
	}

	public static void commonMethod(Pattern<Event,Event> pattern) {
		Pattern<Event,Event> pattern2 = pattern.where(new SimpleCondition<Event>() {
			int numberofTimes = 0;
			@Override
			public boolean filter(Event value) throws Exception {
				numberofTimes ++;
				value.setNumberOfTimes(numberofTimes);
				FileWriter.writerFile(value.toString(),"test2.txt");
				return true;
			}
		});
		simplePattern(getInputFromCollection(16,19,20),pattern2,"first");
	}




}
