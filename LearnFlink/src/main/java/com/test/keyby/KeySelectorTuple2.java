package com.test.keyby;

import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorTuple2 implements KeySelector<String,String> {
	@Override
	public String getKey(String value) throws Exception {
		return null;
	}
}
