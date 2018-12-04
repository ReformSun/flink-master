package com.test.customTrigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import test.TimeAndNumber;

/**
 * 触发器，进行窗口的处理或清除，每个窗口都会拥有一个的Trigger。
 * 还未完全理解每个类具体的作用和怎样灵活的使用
 */
public class CustomTrigger extends Trigger<TimeAndNumber,TimeWindow> {
	@Override
	public TriggerResult onElement(TimeAndNumber element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately 如果窗口最大时间超过当前的水印，窗口数据立即发射
			return TriggerResult.FIRE;
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	}

	@Override
	public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
		return time == window.maxTimestamp() ?
			TriggerResult.FIRE :
			TriggerResult.CONTINUE;
	}

	@Override
	public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
		ctx.deleteEventTimeTimer(window.maxTimestamp());
	}
}
