package com.test.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrint extends RichSinkFunction<String> {
	@Override
	public void invoke(String value) throws Exception {
		if (value != null){
			writerFile(value);
		}

	}
	public static void writerFile(String s) throws IOException {
		Path logFile = Paths.get(".\\src\\main\\resource\\test1.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
