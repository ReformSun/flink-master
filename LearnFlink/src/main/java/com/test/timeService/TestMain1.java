package com.test.timeService;

import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;

public class TestMain1 {
	private static final String lock = "dddd";
	public static void main(String[] args) {
		testMethod1();
	}
	public static void testMethod1(){
		SystemProcessingTimeService systemProcessingTimeService = new SystemProcessingTimeService(new TestAsyncExceptionHandler(),lock);
		TestProcessingTimeCallback testProcessingTimeCallback = new TestProcessingTimeCallback(systemProcessingTimeService,10000);
		testProcessingTimeCallback.start();
	}

	private static class TestProcessingTimeCallback implements ProcessingTimeCallback{
		private final ProcessingTimeService timerService;
		private final long interval;

		public TestProcessingTimeCallback(ProcessingTimeService timerService, long interval) {
			this.timerService = timerService;
			this.interval = interval;
		}

		public void start() {
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
		@Override
		public void onProcessingTime(long timestamp) throws Exception {
			System.out.println("ddd");
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}
}

class TestAsyncExceptionHandler implements AsyncExceptionHandler{
	@Override
	public void handleAsyncException(String message, Throwable exception) {

	}
}
