package com.test.sink;

import com.test.util.FileWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.runtime.types.CRow;

public class CustomCrowSumPrint extends RichSinkFunction<CRow>{
	private Counter sum = null;
	@Override
	public void open(Configuration parameters) throws Exception {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("customSumLastData");
		sum = metricGroup.counter("sum");
		super.open(parameters);
	}

	@Override
	public void invoke(CRow value) throws Exception {
		sum.inc((Long) value.row().getField(0));
		FileWriter.writerFile(value.row().toString(),"outofdateData.txt");
	}
}
