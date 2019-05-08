package com.test.map;

import com.test.util.URLUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestMap implements MapFunction<Row,Integer>{

	private StreamTableEnvironment streamTableEnvironment;

	public TestMap(StreamTableEnvironment streamTableEnvironment) {
		this.streamTableEnvironment = streamTableEnvironment;
	}

	@Override
	public Integer map(Row value) throws Exception {

		Table table = streamTableEnvironment.sqlQuery("select * from table1");
		return 1;
	}


	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get(URLUtil.baseUrl+"test1.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
