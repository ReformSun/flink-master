package com.test.batch.learnMain;

import com.test.batch.function.MovieMapFunction;
import com.test.batch.model.Movie;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * 迭代学习
 */
public class TestMain3 extends AbstractTestMain{
	public static void main(String[] args) throws Exception {
//		testMethod1();
//		testMethod2();
		testMethod3();
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

	public static void testMethod2() throws Exception {
		IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

		DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer i) throws Exception {
				double x = Math.random();
				double y = Math.random();

				return i + ((x * x + y * y < 1) ? 1 : 0);
			}
		});

// Iteratively transform the IterativeDataSet
		DataSet<Integer> count = initial.closeWith(iteration);

		MapOperator<Integer,Double> mapOperator = count.map(new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer count) throws Exception {
				return count / (double) 10000 * 4;
			}
		});
		mapOperator.writeAsText(".\\LearnFlink\\src\\main\\resources\\movie", FileSystem.WriteMode.OVERWRITE);
	}

	public static void testMethod3() {
		int MAX_ITERATION_NUM = 100;
		DataSet<Tuple2<Long, Long>> verticesAsWorkset = generateWorksetWithVertices();
		DataSet<Tuple2<Long, Long>> edges = generateDefaultEdgeDataSet();

		int vertexIdIndex = 0;
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = verticesAsWorkset
			.iterateDelta(verticesAsWorkset, MAX_ITERATION_NUM, vertexIdIndex);

		DataSet<Tuple2<Long, Long>> delta = iteration.getWorkset()
			.join(edges).where(0).equalTo(0)
			.with(new NeighborWithParentIDJoin())
			.join(iteration.getSolutionSet()).where(0).equalTo(0)
			.with(new RootIdFilter());
		DataSet<Tuple2<Long, Long>> finalDataSet = iteration.closeWith(delta, delta);
		sink(finalDataSet);

	}


	public static final class NeighborWithParentIDJoin implements
		JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexAndParent,
									   Tuple2<Long, Long> edgeSourceAndTarget) throws Exception {
			return new Tuple2<Long, Long>(edgeSourceAndTarget.f1, vertexAndParent.f1);
		}
	}

	public static final class RootIdFilter implements FlatJoinFunction<Tuple2<Long, Long>,
            Tuple2<Long, Long>, Tuple2<Long, Long>> {
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old,
						 Collector<Tuple2<Long, Long>> collector) throws Exception {
			if (candidate.f1 < old.f1) {
				collector.collect(candidate);
			}
		}
	}

}
