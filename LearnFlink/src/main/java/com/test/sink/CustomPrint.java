package com.test.sink;

import com.test.util.URLUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrint extends RichSinkFunction<String> {
	private String fileName = "test.txt";

	public CustomPrint(String fileName) {
		if (fileName != null)this.fileName = fileName;
	}

	@Override
	public void invoke(String value) throws Exception {
		if (value != null){
			writerFile(value);
		}

	}
	public void writerFile(String s) throws IOException {
		Path logFile = Paths.get(URLUtil.baseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
