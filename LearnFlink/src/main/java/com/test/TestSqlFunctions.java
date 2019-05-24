package com.test;

import org.apache.calcite.runtime.SqlFunctions;

import java.sql.Timestamp;

public class TestSqlFunctions {
	public static void main(String[] args) {
//		testMethod1();
		testMethod2();
	}

	public static void testMethod1(){
	    long time = 1558459980000L;
		Timestamp timestamp = new Timestamp(time);
		// 转换一次
		long time_1 = SqlFunctions.toLong(timestamp);
		System.out.println(time_1);
		// 转换两次
		Timestamp timestamp1 = SqlFunctions.internalToTimestamp(time_1);
		System.out.println(timestamp1.getTime());
	}

	public static void testMethod2(){
		long time = 1558459980000L;
		Timestamp timestamp1 = SqlFunctions.internalToTimestamp(time);
		System.out.println(timestamp1.getTime());
	}
}
