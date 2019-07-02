package com.test;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.Properties;

public class TestRow {
	public static void main(String[] args) {

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//		System.out.println(parameterTool.toMap().toString());
//		Map map = parameterTool.toMap();
		Properties properties = parameterTool.getProperties();
		properties.get("dd");
		properties.getProperty("c");
	}

	public static void testMethod1(){
	    Row row = new Row(2);
	    row.setField(0,1);
	    row.setField(1,"dd");
//		Row.copy()
	}
}
