package com.test.learnWindows;

/**
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamElement}
 * 事件在传播中会被封装成StreamElement的类
 * {@link org.apache.flink.streaming.api.watermark.Watermark}
 * {@link org.apache.flink.streaming.runtime.streamrecord.LatencyMarker}
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}
 * {@link org.apache.flink.streaming.runtime.streamstatus.StreamStatus}
 */
public class LearmStreamElement {
	public static void main(String[] args) {

	}

	/**
	 * 元素处理顺序
	 * LatencyMarker
	 * StreamRecord
	 * Watermark 这个水印是上一个元素的水印
	 * StreamRecord
	 * WatermarkStreamRecord
	 * Watermark
	 */
	public static void testMethod1(){

	}
}