package com.test.asyncFunction;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class CustomRichAsyncFunction extends RichAsyncFunction{
	@Override
	public void asyncInvoke(Object input, ResultFuture resultFuture) throws Exception {

	}

	@Override
	public void timeout(Object input, ResultFuture resultFuture) throws Exception {

	}
}
