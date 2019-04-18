package com.test.learnMetric.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

public class TestRichFlatMapFunction extends RichFlatMapFunction<Tuple3<String, Integer, Long>,String> {
	private Counter counter;

	@Override
	public void flatMap(Tuple3<String, Integer, Long> value, Collector<String> out) throws Exception {
		counter.inc();
		out.collect(value.toString());
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		RuntimeContext runtimeContext = getRuntimeContext();
		counter = runtimeContext.getMetricGroup().counter(1);
		super.open(parameters);
	}
}
