package com.test;

import com.test.testClass.TestClass1;
import com.test.testClass.TestClass3;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * 对于powermock
 * 对于静态函数、构造函数、私有函数、Final 函数以及系统函数的模拟
 */
@RunWith(PowerMockRunner.class)
public class TestMain3 {
	/**
	 * 对于构造函数的模拟
	 */
	@Test
	public void testMethod1() {
		TestClass3 testClass3 = mock(TestClass3.class);
		try {
			whenNew(TestClass3.class).withAnyArguments().thenReturn(testClass3);
			TestClass3 testClass11 = new TestClass3();
			assertEquals(testClass3.toString(),testClass11.toString());
			verifyNew(TestClass3.class).withNoArguments();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMethod1_1() {
		TestClass3 testClass3 = mock(TestClass3.class);
		try {
			whenNew(TestClass3.class).withArguments("a","b").thenReturn(testClass3);
			TestClass3 testClass11 = new TestClass3();
			assertFalse(testClass3.equals(testClass11));
			TestClass3 testClass31 = new TestClass3("a","b");
			assertTrue(testClass3.equals(testClass31));
			verifyNew(TestClass3.class).withArguments("a","b");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
