package com.test.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class CustomSource<T> extends RichParallelSourceFunction<T> implements
	ResultTypeQueryable<T>,CheckpointListener {
	private final long interval;
	private ProduceData<T> produceData;

	public CustomSource(long interval, ProduceData<T> produceData) {
		this.interval = interval;
		this.produceData = produceData;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		for (int i = 0; i < 100; i++) {
			T data = produceData.getData();
			ctx.collect(data);
			Thread.sleep(interval);
		}
	}

	@Override
	public void cancel() {

	}

	@Override
	public TypeInformation<T> getProducedType() {
		return produceData.getProducedType();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}
}
