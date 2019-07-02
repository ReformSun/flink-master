package com.test.window;

import com.test.util.TimeUtil;

public class TimeWindow {
	public static void main(String[] args) throws InterruptedException {
		long offset = 30000;
		long windowSize = 60000;
		long time = 0L;
		time = TimeUtil.toLong("2019-06-13 1:33:30:000");
		for (int i = 0; i < 10; i++) {
			time += 10000;
			System.out.println(TimeUtil.toDate(getWindowStartWithOffset(time,offset,windowSize)));
		}
	}
	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
	}
}
