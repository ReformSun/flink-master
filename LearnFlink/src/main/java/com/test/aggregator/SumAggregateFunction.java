package com.test.aggregator;

import org.apache.flink.api.common.functions.RichAggregateFunction;

public class SumAggregateFunction<IN, ACC, OUT> extends RichAggregateFunction<IN, ACC, OUT>{
	@Override
	public ACC createAccumulator() {
		return null;
	}

	@Override
	public ACC add(IN value, ACC accumulator) {
		return null;
	}

	@Override
	public OUT getResult(ACC accumulator) {
		return null;
	}

	@Override
	public ACC merge(ACC a, ACC b) {
		return null;
	}
}
