package com.test.customEvictor;

import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import test.TimeAndNumber;

import java.util.*;

/**
 * “驱逐者”，类似filter作用。在Trigger触发之后，window被处理前，EVictor用来处理窗口中无用的元素。
 */
public class CustomEvictor implements Evictor<TimeAndNumber,TimeWindow> {
	@Override
	public void evictBefore(Iterable<TimestampedValue<TimeAndNumber>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
		Set<TimestampedValue<TimeAndNumber>> list = new HashSet<>();
			Iterator<TimestampedValue<TimeAndNumber>> iterator = elements.iterator();
			while (iterator.hasNext()){
				TimestampedValue<TimeAndNumber> timestampedValue = iterator.next();
				if (timestampedValue.getValue().getNumber() != 1){
					list.add(timestampedValue);
				}
			}
		FluentIterable<TimestampedValue<TimeAndNumber>> fluentIterable = FluentIterable.from(list);
	}

	@Override
	public void evictAfter(Iterable<TimestampedValue<TimeAndNumber>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

	}
}
