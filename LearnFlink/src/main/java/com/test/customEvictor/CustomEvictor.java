package com.test.customEvictor;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import test.TimeAndNumber;

/**
 * “驱逐者”，类似filter作用。在Trigger触发之后，window被处理前，EVictor用来处理窗口中无用的元素。
 */
public class CustomEvictor implements Evictor<TimeAndNumber,TimeWindow> {
	@Override
	public void evictBefore(Iterable<TimestampedValue<TimeAndNumber>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

	}

	@Override
	public void evictAfter(Iterable<TimestampedValue<TimeAndNumber>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

	}
}
