package com.test.sdn;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple8;

public class CustomKeySelector implements KeySelector<Tuple8<String,String,String,
	String,Long,
	String,
	Boolean,
	Long>, String> {
	@Override
	public String getKey(Tuple8<String, String, String, String, Long, String, Boolean, Long> value) throws Exception {
		return value.f3;
	}
}
