package com.test.customAssignTAndW;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampTyple3 implements AssignerWithPunctuatedWatermarks<Tuple3<String,Integer,Long>>{

	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	@Nullable
	@Override
	public Watermark checkAndGetNextWatermark(Tuple3<String, Integer, Long> lastElement, long extractedTimestamp) {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
		long timestamp = element.getField(2);
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
