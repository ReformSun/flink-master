package com.test.batch.learnMain;

import com.test.batch.function.MovieMapFunction;
import com.test.batch.model.Movie;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 迭代学习
 */
public class TestMain3 extends AbstractTestMain{
	public static void main(String[] args) {
		testMethod1();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1() {
		IterativeDataSet<Tuple3<Long, String, String>> lines = getMovieData("test.csv").iterate(100);
		DataSet<Movie> dataSet = lines.map(new MovieMapFunction());
//		DataSet<Movie> dataSet1 = lines.closeWith(dataSet);
		sink(dataSet);
	}
}
