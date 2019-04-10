package com.test.jsonFotmats;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowFormatFactory;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestMain1 {
	public static void main(String[] args) {
		testMethod1();
	}

	public static void testMethod1() {
		JsonRowFormatFactory jsonRowFormatFactory = new JsonRowFormatFactory();
		Map<String,String> properties = new HashMap<>();
		properties.put("aa","String");
		JsonRowDeserializationSchema jsonRowDeserializationSchema = (JsonRowDeserializationSchema) jsonRowFormatFactory.createDeserializationSchema(properties);
		String s = "{\"a\":\"ddd\"}";
		try {
			Row row = jsonRowDeserializationSchema.deserialize(s.getBytes());
			System.out.println(row.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
