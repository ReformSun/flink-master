package com.test.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeySelectorTuple<T extends Tuple,F> implements KeySelector<T,F> {
	private int index;

	public KeySelectorTuple(int index) {
		this.index = index;
	}

	@Override
	public F getKey(T value) throws Exception {
		return value.getField(index);
	}
}
