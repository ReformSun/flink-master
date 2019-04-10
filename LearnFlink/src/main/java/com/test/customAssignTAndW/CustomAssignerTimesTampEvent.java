package com.test.customAssignTAndW;

import model.Event;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampEvent implements AssignerWithPunctuatedWatermarks<Event> {
	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	@Nullable
	@Override
	public Watermark checkAndGetNextWatermark(Event lastElement, long extractedTimestamp) {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Event element, long previousElementTimestamp) {
		long timestamp = element.getTime();
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
