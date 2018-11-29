package com.test.customAssignTAndW;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import test.TimeAndNumber;

import javax.annotation.Nullable;

/**
 * watermark 水位线的概念和理解
 * 水位线是用来处理乱序事件的
 * 一种业务场景：当由于网络原因，被压原因造成的数据延迟到达，这样的数据叫 乱序数据（out-of-order）或者 晚到元素 （late element）
 * 对于这样的场景的数据 窗口触发不能够无限期等待下去，所以设置水位线的概念
 *
 * 生成水位线主要有两个大类
 *   AssignerWithPeriodicWatermarks
 *
 *   AssignerWithPunctuatedWatermarks
 *
 *
 *
 * 疑问：当数据到达水位线时，窗口被触发，但是还是有窗口内的数据没有及时到达，在下个窗口内才到达，这个数据怎么统计
 *
 *
 */
public class CustomAssignerTimestampsAndWatermark implements AssignerWithPeriodicWatermarks<TimeAndNumber>{
	private long maxOutOfOrderness = 3500L;
	private long currentMaxTimestamp;

	@Nullable
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(TimeAndNumber element, long previousElementTimestamp) {
		long timestamp = element.getTimestamp();
		currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
		return timestamp;
	}
}
