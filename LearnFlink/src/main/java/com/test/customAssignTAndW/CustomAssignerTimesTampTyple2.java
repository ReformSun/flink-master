package com.test.customAssignTAndW;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampTyple2<T,F> implements AssignerWithPunctuatedWatermarks<Tuple2<T,F>> {
	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	private int index;

	public CustomAssignerTimesTampTyple2(int index) {
		this.index = index;
	}

	@Nullable
	@Override
	public Watermark checkAndGetNextWatermark(Tuple2<T,F> lastElement, long extractedTimestamp) {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Tuple2<T,F> element, long previousElementTimestamp) {
		long timestamp = (long)element.getField(index);
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
