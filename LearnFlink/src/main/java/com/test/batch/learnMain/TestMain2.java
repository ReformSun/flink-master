package com.test.batch.learnMain;

import com.test.batch.function.MovieFilterFunction;
import com.test.batch.function.MovieMapFunction;
import com.test.batch.model.Movie;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class TestMain2 extends AbstractTestMain {
	public static void main(String[] args) throws Exception {
//		testMethod1();
//		testMethod2();
//		testMethod2_1();
//		testMethod2_2();
//		testMethod2_3();
//		testMethod2_4();
		testMethod5();


		env.execute();
	}

	/**
	 *
	 */
	public static void testMethod1() {
		DataSet<Tuple3<Long, String, String>> lines = getMovieData(null);
		DataSet<Movie> dataSet = lines.map(new MovieMapFunction());
		DataSet<Movie> dataSet1 = dataSet.filter(new MovieFilterFunction());
		dataSet1.writeAsText(".\\LearnFlink\\src\\main\\resources\\movie", FileSystem.WriteMode.OVERWRITE);
	}

	/**
	 * lineDelimiter
	 */
	public static void testMethod2() {
		CsvReader csvReader = getTestData(null)
			.ignoreFirstLine() // 忽略第一行
			.ignoreInvalidLines() // 忽略无效的行
			.includeFields(true,true,true)
			;
		DataSet<Tuple3<Long, String, String>> dataSet = csvReader.types(Long.class,String.class,String.class);
		DataSet<Movie> dataSet2 = dataSet.map(new MovieMapFunction());
		sink(dataSet2);
	}

	/**
	 * includeFields 的学习 过滤不需要的列
	 */
	public static void testMethod2_1() {
		CsvReader csvReader = getTestData(null)
			.ignoreFirstLine() // 忽略第一行
			.ignoreInvalidLines() // 忽略无效的行
			.includeFields(false,true,true) // 第一列过滤掉，包含第二列和第三列
			;
		DataSet<Tuple2<String, String>> dataSet = csvReader.types(String.class,String.class);
		sink(dataSet);
	}

	/**
	 * fieldDelimiter 一行中各个字段的限定符 一般为，
	 * lineDelimiter 每一行的限定符，一般根据换行符
	 */
	public static void testMethod2_2() {
		CsvReader csvReader = getTestData("test2.csv")
			.ignoreFirstLine() // 忽略第一行
			.ignoreInvalidLines() // 忽略无效的行
			.includeFields(false,true,true) // 第一列过滤掉，包含第二列和第三列
			.fieldDelimiter("|") // csv 文件中一行的切分限定符，一般是采用，
			.lineDelimiter("\n")
			;
		DataSet<Tuple2<String, String>> dataSet = csvReader.types(String.class,String.class);
		sink(dataSet);
	}

	/**
	 * parseQuotedStrings 当一个字符串中含有限定字符时，为了把这个字段看成一个字段，我问可以启动启动解析引号字符串，就是把此字符串放入指定的解析引号中，系统就会认为此字符串为一个字段
	 * 比如：
	 * movieId,title,genres
	 * 1,Toy Story (1995),"Adventure,Animation"
	 * 如果不调用parseQuotedStrings('"') 方法，返回的这行值为1     Toy Story (1995)     "Adventure
	 * 如果调用1    Toy Story (1995)     Adventure,Animation
	 *
	 */
	public static void testMethod2_3() {
		CsvReader csvReader = getTestData(null)
			.ignoreFirstLine() // 忽略第一行
			.ignoreInvalidLines() // 忽略无效的行
			.includeFields(false,true,true) // 第一列过滤掉，包含第二列和第三列
			.parseQuotedStrings('"')
			;
		DataSet<Tuple2<String, String>> dataSet = csvReader.types(String.class,String.class);
		sink(dataSet);
	}

	/**
	 * ignoreComments("2") 忽略csv中一行以2开头的字符串
	 * 比如：
	 * movieId,title,genres
	 * 1,Toy Story (1995),"Adventure,Animation"
	 * 2,Jumanji (1995),Adventure|Children|Fantasy
	 * 这个第三行将被忽略掉
	 *
	 */
	public static void testMethod2_4() {
		CsvReader csvReader = getTestData(null)
			.ignoreFirstLine() // 忽略第一行
			.ignoreInvalidLines() // 忽略无效的行
			.includeFields(true,true,true) // 第一列过滤掉，包含第二列和第三列
			.parseQuotedStrings('"')
			.ignoreComments("2")
			;
		DataSet<Tuple3<Long,String,String>> dataSet = csvReader.types(Long.class,String.class,String.class);
		sink(dataSet);
	}

	/**
	 * Recursive Traversal of the Input Path Directory
	 * 递归遍历输入路径目录
	 */
	public static void testMethod3() {
		Configuration parameters = new Configuration();
		parameters.setBoolean("recursive.file.enumeration", true);
		DataSet<String> logs = env.readTextFile(".\\LearnFlink\\src\\main\\resources\\testCsv1")
			.withParameters(parameters);
	}

	/**
	 * 读取压缩文件
	 */
	public static void testMethod4() {
		
	}

	/**
	 * ASCENDING 升序
	 * DESCENDING 降序
	 *
	 */
	public static void testMethod5() {
		DataSet<Tuple3<Long, String, String>> lines = getMovieData(null);
		DataSet<Movie> dataSet = lines.map(new MovieMapFunction());
		dataSet.sortPartition(new KeySelector<Movie, Long>() {
			@Override
			public Long getKey(Movie value) throws Exception {
				return value.getMovieId();
			}
		}, Order.ASCENDING);
		sink(dataSet);
	}


}
