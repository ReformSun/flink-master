package com.test.batch.learnMain;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class AbstractTestMain {
	public static final ExecutionEnvironment env;
	static {
		env = ExecutionEnvironment.createLocalEnvironment();
	}

	public static DataSet<Tuple3<Long, String, String>> getMovieData(String fileName){
		String path;
		if (fileName == null)path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\movies.csv";
		else path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\" + fileName;
		DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile(path)
			.ignoreFirstLine()
			.parseQuotedStrings('"')
			.ignoreInvalidLines()
			.types(Long.class,String.class,String.class);
		return lines;
	}

	public static CsvReader getTestData(String fileName){
		String path;
		if (fileName == null)path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\test.csv";
		else path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\" + fileName;
		return env.readCsvFile(path);
	}

	public static void sink(DataSet dataSet){
		dataSet.writeAsText(".\\LearnFlink\\src\\main\\resources\\movie", FileSystem.WriteMode.OVERWRITE);
	}

	public static DataSet<Tuple2<Long, Long>> generateWorksetWithVertices(){
		String path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\Vertices.csv";
		DataSet<Tuple2<Long,Long>> lines = env.readCsvFile(path)
			.ignoreFirstLine()
			.parseQuotedStrings('"')
			.ignoreInvalidLines()
			.types(Long.class,Long.class);
		return lines;
	}

	public static DataSet<Tuple2<Long, Long>> generateDefaultEdgeDataSet(){
		String path = ".\\LearnFlink\\src\\main\\resources\\testCsv\\Edges.csv";
		DataSet<Tuple2<Long,Long>> lines = env.readCsvFile(path)
			.ignoreFirstLine()
			.parseQuotedStrings('"')
			.ignoreInvalidLines()
			.types(Long.class,Long.class);
		return lines;
	}
}
