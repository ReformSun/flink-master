package com.test.customAssignTAndW;

import com.test.util.FileWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.IOException;

public class CustomAssignerTimesTampTyple3<T,F,J> implements AssignerWithPunctuatedWatermarks<Tuple3<T,F,J>>{
	/**
	 * 单位毫秒
	 */
	private long maxOutOfOrderness = 0;
	private long currentMaxTimestamp;
	private int index = 0;

	public CustomAssignerTimesTampTyple3(int index) {
		this.index = index;
	}

	public CustomAssignerTimesTampTyple3(long maxOutOfOrderness) {
		this.maxOutOfOrderness = maxOutOfOrderness;
	}

	public CustomAssignerTimesTampTyple3(long maxOutOfOrderness, int index) {
		this.maxOutOfOrderness = maxOutOfOrderness;
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
		if (index == Long.MAX_VALUE){
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
