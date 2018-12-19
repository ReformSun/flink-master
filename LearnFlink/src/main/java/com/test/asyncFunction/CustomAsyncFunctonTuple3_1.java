package com.test.asyncFunction;

import com.test.cache.CustomCacheDataBaseConf;
import com.test.cache.CustomCacheDataBaseImp;
import com.test.cache.Value;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.List;

public class CustomAsyncFunctonTuple3_1 extends RichAsyncFunction<Tuple3<String,Integer,Long>, Tuple3> {
	private  CustomCacheDataBaseImp customCacheDataBaseImp;

	public CustomAsyncFunctonTuple3_1(CustomCacheDataBaseConf customCacheDataBaseConf) throws Exception {
		this.customCacheDataBaseImp = CustomCacheDataBaseImp.creatCache(customCacheDataBaseConf);
	}

	@Override
	public void asyncInvoke(Tuple3<String, Integer, Long> input, ResultFuture<Tuple3> resultFuture) throws Exception {
		List list = new ArrayList<Tuple3>();
		Tuple3 tuple3 = null;
		System.out.println(Thread.currentThread().getName() + " asyncInvoke ddddd");
		Value value = customCacheDataBaseImp.getValue(input.getField(1));
		tuple3 = new Tuple3(input.f0,value.getValue(),input.f2);
		list.add(tuple3);
		System.out.println(Thread.currentThread().getName() + " asyncInvoke");
		resultFuture.complete(list);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		customCacheDataBaseImp.initDataBase();
		customCacheDataBaseImp.initCache();
		super.open(parameters);
	}
}
