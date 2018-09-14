package com.test.batch.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;

public class CustomBatchSink2 extends RichOutputFormat<Tuple> {
	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

	}

	@Override
	public void writeRecord(Tuple record) throws IOException {

	}

	@Override
	public void close() throws IOException {

	}
}
