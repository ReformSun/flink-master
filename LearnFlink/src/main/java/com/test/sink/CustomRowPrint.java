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

public class CustomRowPrint extends RichSinkFunction<Row> {
	private String fileName;
	public CustomRowPrint(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void invoke(Row value) throws Exception {
		writerFile(value.toString());
	}
	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get( URLUtil.baseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
