package com.test;

import com.test.util.URLUtil;
import org.apache.flink.util.InstantiationUtil;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * {@link org.apache.flink.util.InstantiationUtil}
 */
public class TestInstantiationUtil {
	public static void main(String[] args) {
//		testMethod1();
		testMethod2();
	}
	// 实例化类的编码
	public static void testMethod1() {
	    TestClass testClass = new TestClass("ccc");
		try {
			byte[] bytes = InstantiationUtil.serializeObject(testClass);
			File file = new File(URLUtil.baseUrl +"test.snapshot");
			file.createNewFile();
			Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	// 实例化类的反编码
	public static void testMethod2(){
		File file = new File(URLUtil.baseUrl +"test.snapshot");
		try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(file)))) {
			TestClass testClass = (TestClass) InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			System.out.println(testClass.getS());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
