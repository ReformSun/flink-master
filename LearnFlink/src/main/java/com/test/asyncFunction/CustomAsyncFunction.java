package com.test.asyncFunction;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

public class CustomAsyncFunction implements AsyncFunction {
	@Override
	public void asyncInvoke(Object input, ResultFuture resultFuture) throws Exception {

	}

	@Override
	public void timeout(Object input, ResultFuture resultFuture) throws Exception {

	}
}
