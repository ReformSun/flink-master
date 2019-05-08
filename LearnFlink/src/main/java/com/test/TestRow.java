package com.test;

import org.apache.flink.types.Row;

public class TestRow {
	public static void main(String[] args) {

	}

	public static void testMethod1(){
	    Row row = new Row(2);
	    row.setField(0,1);
	    row.setField(1,"dd");


	}
}
