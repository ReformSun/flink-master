package com.test.asyncFunction;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

public class CustomRichAsyncFunction extends RichAsyncFunction<Row,Row>{
	@Override
	public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

	}

	@Override
	public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

	}
}
