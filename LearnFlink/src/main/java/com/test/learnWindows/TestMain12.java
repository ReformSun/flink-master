package com.test.learnWindows;


import com.test.sink.CustomPrint;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class TestMain12 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		testMethod1(sEnv);
		sEnv.execute();

	}

	public static void testMethod1(StreamExecutionEnvironment sEnv) {
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

		DataStreamSource<Row> dataStreamSource = sEnv.createInput(jdbcInputFormat);
		DataStream<String> dataStream = dataStreamSource.map(new MapFunction<Row, String>() {
			@Override
			public String map(Row value) throws Exception {
				return value.toString();
			}
		});
		dataStream.addSink(new CustomPrint("test.txt"));

	}

	public static void testMethod2(StreamExecutionEnvironment sEnv) {
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"Kumar"};
		queryParameters[1] = new String[]{"Tan Ah Teck"};
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
			.setQuery("select * from books WHERE author = ?")
			.setUsername("apm")
			.setPassword("apm")
			.setRowTypeInfo(rowTypeInfo)
			.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
			.finish();
	}

}
