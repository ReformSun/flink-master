package com.test;

import com.test.testClass.TestClass1;
import com.test.testClass.TestInterface;
import com.test.testClass.TestInterface2;
import com.test.testClass.TestInterface3;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;


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

	/**
	 * 模拟有泛型的接口
	 */
	@Test
	public void testMethod2() {
		testInterface2 = Mockito.mock(TestInterface2.class);
		Mockito.when(testInterface2.testMethod2()).thenReturn("aa");
		assertEquals(testInterface2.testMethod2(),"aa");
	}

	/**
	 * 模拟没有声明为测试类属性，并用@Mock注解的方式模拟接口
	 */
	@Test
	public void testMethod3() {
		TestInterface2 testInter = Mockito.mock(TestInterface2.class);
		Mockito.when(testInter.testMethod2()).thenReturn("aa");
		assertEquals(testInter.testMethod2(),"aa");
	}

	/**
	 * 模拟接口的无返回值的方法
	 */
	@Test
	public void testMethod4() {
		TestInterface3 testInterface3 = mock(TestInterface3.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				System.out.println("你好");
				return null;
			}
		}).when(testInterface3).testMethod1();

		testInterface3.testMethod1();
	}

	@Test
	public void testMethod5() {
		TestClass1 testClass1 = mock(TestClass1.class);
		try {
			whenNew(TestClass1.class).withAnyArguments().thenReturn(testClass1);

			TestClass1 testClass11 = mock(TestClass1.class);
			assertEquals(testClass1.toString(),testClass11.toString());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 模拟方法抛出异常
	 */
	@Test
	public void testMethod6() {
		TestClass1 testClass1 = mock(TestClass1.class);
		try {
			when(testClass1.testMethod1()).thenThrow(new Exception("cc"));
			testClass1.testMethod1();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 无返回值抛出异常
	 */
	@Test
	public void testMethod6_1() {
		TestClass1 testClass1 = mock(TestClass1.class);
		try {
			doThrow(new Exception("dd")).when(testClass1).testMethod3();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 模拟方法参数的匹配
	 */
	@Test
	public void testMethod7() {
		TestClass1 testClass1 = mock(TestClass1.class);
		when(testClass1.testMethod2(anyInt(),anyLong())).thenReturn("cc");
	}

	/**
	 * 另一种写法
	 */
	@Test
	public void testMethod7_1() {
		TestClass1 testClass1 = mock(TestClass1.class);
		when(testClass1.testMethod2(anyInt(),anyLong())).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocationOnMock) throws Throwable {
				return "dd";
			}
		});
		assertEquals(testClass1.testMethod2(1,2L),"dd");
	}


}
