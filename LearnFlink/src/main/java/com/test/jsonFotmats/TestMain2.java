package com.test.jsonFotmats;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * flink 为什么使用jackson2
 * https://www.jianshu.com/p/299e8cbea01e
 */
public class TestMain2 {
	public static void main(String[] args) {
		testMethod1();
	}

	public static void testMethod1() {
		ObjectMapper objectMapper = new ObjectMapper();
		String json = "{\"a\":1}";
		try {
			JsonNode jsonNode = objectMapper.readTree(json);
			System.out.println(jsonNode.get("a"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
