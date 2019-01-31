package com.test;

import com.test.testClass.TestClass2;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 * 校验行为
 */
public class TestMain2 {
	/**
	 * 验证方法被调用一次
	 */
	@Test
	public void testMethod1() {
		TestClass2 testClass2 = mock(TestClass2.class);
		testClass2.testMethod1();
//		testClass2.testMethod1();
		verify(testClass2).testMethod1();
	}

	/**
	 * 验证方法被调用一次
	 */
	@Test
	public void testMethod1_1() {
		TestClass2 testClass2 = mock(TestClass2.class);
		testClass2.testMethod1();
		verify(testClass2,times(1)).testMethod1();
	}
}
