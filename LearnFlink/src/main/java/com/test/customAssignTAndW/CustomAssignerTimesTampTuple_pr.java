package com.test.customAssignTAndW;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampTuple_pr<T extends Tuple> implements AssignerWithPeriodicWatermarks<T> {
	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	private int index;

	public CustomAssignerTimesTampTuple_pr(int index) {
		this.index = index;
	}

	@Nullable
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(T element, long previousElementTimestamp) {
		long timestamp = (long)element.getField(index);
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
