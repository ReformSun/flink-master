package com.test.customAssignTAndW;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampTyple8 implements AssignerWithPunctuatedWatermarks<Tuple8<String, String, String, String, Long, String, Boolean, Long>> {
	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	private int index;

	public CustomAssignerTimesTampTyple8(int index) {
		this.index = index;
	}

	@Nullable
	@Override
	public Watermark checkAndGetNextWatermark(Tuple8<String, String, String, String, Long, String, Boolean, Long> lastElement, long extractedTimestamp) {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Tuple8<String, String, String, String, Long, String, Boolean, Long> element, long previousElementTimestamp) {
		long timestamp = (long)element.getField(index);
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
