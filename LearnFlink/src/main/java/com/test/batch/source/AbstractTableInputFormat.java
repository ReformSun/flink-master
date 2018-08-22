package com.test.batch.source;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;

public class AbstractTableInputFormat<T> extends RichInputFormat<T, LocatableInputSplit> {
	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public LocatableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return new LocatableInputSplit[0];
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(LocatableInputSplit[] inputSplits) {
		return null;
	}

	@Override
	public void open(LocatableInputSplit split) throws IOException {

	}

	@Override
	public boolean reachedEnd() throws IOException {
		return false;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
