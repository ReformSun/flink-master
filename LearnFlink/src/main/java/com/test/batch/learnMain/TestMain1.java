package com.test.batch.learnMain;

import com.test.sink.CustomBatchSink;
import com.test.sink.CustomPrint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class TestMain1 {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(environment);

		testMethod2(tableEnv,environment);

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
		Table result=tableEnv.sqlQuery("");
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

	public static void testMethod2(BatchTableEnvironment tableEnv,ExecutionEnvironment environment) {
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"小赵"};
		queryParameters[1] = new String[]{"小李"};
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
			.setQuery("select * from figure WHERE username = ?")
			.setUsername("apm")
			.setPassword("apm")
			.setRowTypeInfo(rowTypeInfo)
			.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
			.finish();
		DataSet<Row> source = environment.createInput(jdbcInputFormat);
		tableEnv.registerDataSetInternal("ddd", source);
		Table result=tableEnv.sqlQuery("select * from ddd");
		DataSet<Row> dataSet = tableEnv.toDataSet(result, Row.class);
		dataSet.output(
			new CustomBatchSink()
		);

	}
}
