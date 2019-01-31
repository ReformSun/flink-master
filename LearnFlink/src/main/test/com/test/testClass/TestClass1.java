package com.test.testClass;

public class TestClass1 {
	/**
	 * 有返回值抛出异常
	 */
	public String testMethod1() throws Exception {
		throw new Exception("aa");
	}

	/**
	 * 无返回值抛出异常
	 */
	public void testMethod3() throws Exception {
		throw new Exception("aa");
	}

	public String testMethod2(int i,long l) {
		System.out.println(i + l);
		return null;
	}

}
