package com.test.batch.learnMain;

import com.test.batch.sink.CustomBatchSink;
import com.test.batch.sink.CustomBatchSink2;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import scala.collection.Seq;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;


public class TestMain1 {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(environment);


		testMethod3(tableEnv,environment);

		environment.execute();

	}

	public static void testMethod1(BatchTableEnvironment tableEnv,ExecutionEnvironment environment) {
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
			.setUsername("apm")
			.setPassword("apm")
			.setQuery("select * from figure")
			.setRowTypeInfo(rowTypeInfo)
			.setFetchSize(3)
			.finish();

		DataSet<Row> source = environment.createInput(jdbcInputFormat);
		tableEnv.registerDataSetInternal("ddd", source);
		Table result=tableEnv.sqlQuery("SELECT COUNT(age),CONCAT(sex,username),TUMBLE_START(createtime, INTERVAL '1' MINUTE) FROM tableName GROUP BY TUMBLE(createtime, INTERVAL '1' MINUTE),sex,username");
		DataSet<Row> dataSet = tableEnv.toDataSet(result, Row.class);

		dataSet.output(
			// build and configure OutputFormat
			JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:derby:memory:persons")
				.setQuery("insert into persons (name, age, height) values (?,?,?)")
				.finish()
		);



	}

	/**
	 * partition 分区概念
	 * 测试心得
	 * 现象
	 * 设置平行度为1时  可处理数据量为 27
	 * 设置平行度为2时 每个分区 可处理数据量为 13 14
	 * 设置平行度为3时 每个分区 可处理数据量为 9 9 9
	 * 理解 parallel dataflows
	 * 一个flink程序在内部是呈现平行和分布式的，在执行期间，一个流会有一个或者多个流分区 和每一个执行算子有一个或者多个算子子任务
	 * 这个算子子任务相对另一个是独立，执行在不同的线程中并且可能执行在不同的机器或者容器中
	 *
	 */
	public static void testMethod2(BatchTableEnvironment tableEnv,ExecutionEnvironment environment) throws Exception {
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP
		};

		String[] fieldNames = {"username","sex","age","createtime"};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes,fieldNames);
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"小赵"};
		queryParameters[1] = new String[]{"小李"};
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
//			.setQuery("select username,sex,age,createtime from figure WHERE username = ?")
			.setQuery("select username,sex,age,createtime from figure ORDER BY createtime")
			.setUsername("apm")
			.setPassword("apm")
			.setRowTypeInfo(rowTypeInfo)
			.setFetchSize(10)
//			.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
			.finish();
		DataSet<Row> source = environment.createInput(jdbcInputFormat);

		tableEnv.registerDataSetInternal("ddd", source);


//		Table result=tableEnv.sqlQuery("select sum(age),sex from ddd group by sex");

		Table result=tableEnv.scan("ddd").window(Tumble.over("1.minutes").on("createtime").as("w")).groupBy("w,sex").select("w.end ,age.");
//		Table table = result.groupBy("sex").select("timee,age.sum.sex");

		RowTypeInfo rowTypeInfos = new RowTypeInfo(Types.SQL_TIMESTAMP, Types.INT);
		TupleTypeInfo<Tuple3<Timestamp,String, Integer>> tupleType = new TupleTypeInfo<>(
			Types.SQL_TIMESTAMP,
			Types.INT,
			Types.STRING
		);
		DataSet<Row> dataSet = tableEnv.toDataSet(result,rowTypeInfos);
//		DataSet<Tuple3<Timestamp,String, Integer>> dataSet = tableEnv.toDataSet(result,tupleType);

//		System.out.println( "cccccccccccccccccccccccccccccccccccccccccccccccccccc" + dataSet.count());
		dataSet.output(
			new CustomBatchSink()
		).setParallelism(1);

//		dataSet.output(
//			new RichOutputFormat<Tuple3<Timestamp,String, Integer>>() {
//				@Override
//				public void configure(Configuration parameters) {
//
//				}
//
//				@Override
//				public void open(int taskNumber, int numTasks) throws IOException {
//
//				}
//
//				@Override
//				public void writeRecord(Tuple3<Timestamp,String, Integer> record) throws IOException {
//
//				}
//
//				@Override
//				public void close() throws IOException {
//
//				}
//			}
//		).setParallelism(1);

	}
	public static void testMethod3(BatchTableEnvironment tableEnv, ExecutionEnvironment environment) throws Exception {
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP
		};

		String[] fieldNames = {"username","sex","age","createtime"};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes,fieldNames);
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"小赵"};
		queryParameters[1] = new String[]{"小李"};
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
//			.setQuery("select username,sex,age,createtime from figure WHERE username = ?")
			.setQuery("select username,sex,age,createtime from figure ORDER BY createtime")
			.setUsername("apm")
			.setPassword("apm")
			.setRowTypeInfo(rowTypeInfo)
			.setFetchSize(10)
//			.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
			.finish();
		DataSet<Row> source = environment.createInput(jdbcInputFormat);

		tableEnv.registerDataSetInternal("ddd", source);


		Table result=tableEnv.sqlQuery("SELECT COUNT(age),CONCAT(sex,username),TUMBLE_START(createtime, INTERVAL '1' MINUTE) FROM ddd GROUP BY TUMBLE(createtime, INTERVAL '1' MINUTE),sex,username");

//		Table result=tableEnv.scan("ddd").window(Tumble.over("1.minutes").on("createtime").as("w")).groupBy("w,sex").select("w.end ,age.sum,sex");
//		Table table = result.groupBy("sex").select("timee,age.sum.sex");

		RowTypeInfo rowTypeInfos = new RowTypeInfo(Types.LONG, Types.STRING,Types.SQL_TIMESTAMP);
		TupleTypeInfo<Tuple3<Timestamp,String, Integer>> tupleType = new TupleTypeInfo<>(
			Types.SQL_TIMESTAMP,
			Types.INT,
			Types.STRING
		);
		DataSet<Row> dataSet = tableEnv.toDataSet(result,rowTypeInfos);
//		DataSet<Tuple3<Timestamp,String, Integer>> dataSet = tableEnv.toDataSet(result,tupleType);

//		System.out.println( "cccccccccccccccccccccccccccccccccccccccccccccccccccc" + dataSet.count());
		dataSet.output(
			new RichOutputFormat<Row>() {
				@Override
				public void configure(Configuration parameters) {

				}

				@Override
				public void open(int taskNumber, int numTasks) throws IOException {

				}

				@Override
				public void writeRecord(Row record) throws IOException {
					System.out.println("dddd" + record.toString());
				}

				@Override
				public void close() throws IOException {

				}
			}
		).setParallelism(1);

//		dataSet.output(
//			new RichOutputFormat<Tuple3<Timestamp,String, Integer>>() {
//				@Override
//				public void configure(Configuration parameters) {
//
//				}
//
//				@Override
//				public void open(int taskNumber, int numTasks) throws IOException {
//
//				}
//
//				@Override
//				public void writeRecord(Tuple3<Timestamp,String, Integer> record) throws IOException {
//
//				}
//
//				@Override
//				public void close() throws IOException {
//
//				}
//			}
//		).setParallelism(1);

	}


}
