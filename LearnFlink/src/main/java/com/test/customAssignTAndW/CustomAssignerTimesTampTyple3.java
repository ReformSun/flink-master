package com.test.customAssignTAndW;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomAssignerTimesTampTyple3<T,F,J> implements AssignerWithPunctuatedWatermarks<Tuple3<T,F,J>>{

	private long maxOutOfOrderness = 0L;
	private long currentMaxTimestamp;
	private int index = 0;

	public CustomAssignerTimesTampTyple3(int index) {
		this.index = index;
	}

	public CustomAssignerTimesTampTyple3() {
	}

	@Nullable
	@Override
	public Watermark checkAndGetNextWatermark(Tuple3<T,F,J> lastElement, long extractedTimestamp) {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Tuple3<T,F,J> element, long previousElementTimestamp) {
		long timestamp;
		if (index == 0){
			timestamp = element.getField(2);
		}else {
			timestamp = element.getField(index);
		}

		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
