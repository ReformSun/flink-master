package com.test;

import com.test.testClass.TestInterface;
import com.test.testClass.TestInterface2;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.junit.Assert.*;


public class TestMain {

	@Mock
	TestInterface testInterface;

	@Mock
	TestInterface2 testInterface2;

	/**
	 * Mockito 模拟接口类
	 */
	@Test
	public void testMethod1() {
		testInterface = Mockito.mock(TestInterface.class);
		Mockito.when(testInterface.testMethod()).thenReturn("add");
		assertEquals(testInterface.testMethod(),"add");
	}

	@Test
	public void testMethod2() {
		testInterface2 = Mockito.mock(TestInterface2.class);
		Mockito.when(testInterface2.testMethod2()).thenReturn("aa");
		assertEquals(testInterface2.testMethod2(),"aa");
	}


}
