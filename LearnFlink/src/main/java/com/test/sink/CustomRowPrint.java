package com.test.sink;

import com.test.util.URLUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;

public class CustomRowPrint extends RichSinkFunction<Row> {
	private String fileName;
	public CustomRowPrint(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void invoke(Row value) throws Exception {
		Timestamp timestamp = (Timestamp) value.getField(1);
		writerFile(value.getField(0) + "," + timestamp.getTime());
	}
	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get( URLUtil.baseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
