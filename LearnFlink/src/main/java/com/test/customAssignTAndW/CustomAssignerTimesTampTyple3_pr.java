package com.test.customAssignTAndW;

import com.test.util.FileWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.IOException;

public class CustomAssignerTimesTampTyple3_pr<T,F,J> implements AssignerWithPeriodicWatermarks<Tuple3<T,F,J>> {
	private long maxOutOfOrderness = 0;
	private long currentMaxTimestamp;
	private int index = 0;

	public CustomAssignerTimesTampTyple3_pr(long maxOutOfOrderness, int index) {
		this.maxOutOfOrderness = maxOutOfOrderness;
		this.index = index;
	}

	@Nullable
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(Tuple3<T, F, J> element, long previousElementTimestamp) {
		long timestamp;
		if (index == 0){
			timestamp = element.getField(2);
		}else {
			timestamp = element.getField(index);
		}
		try {
			FileWriter.writerFile(element,"test2.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
