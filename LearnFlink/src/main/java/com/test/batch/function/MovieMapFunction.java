package com.test.batch.function;

import com.test.batch.model.Movie;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MovieMapFunction implements MapFunction<Tuple3<Long, String, String>,Movie> {
	@Override
	public Movie map(Tuple3<Long, String, String> value) throws Exception {
		return new Movie(value.f0,value.f1,value.f2);
	}
}
