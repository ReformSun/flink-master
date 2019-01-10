package com.test.batch.function;

import com.test.batch.model.Movie;
import org.apache.flink.api.common.functions.FilterFunction;

public class MovieFilterFunction implements FilterFunction<Movie>{
	@Override
	public boolean filter(Movie value) throws Exception {
		return value.getGenres().contains("Action");
	}
}
