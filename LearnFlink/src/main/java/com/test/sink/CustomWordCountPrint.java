package com.test.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import test.SunWordWithCount;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomWordCountPrint extends RichSinkFunction<SunWordWithCount> {
	@Override
	public void invoke(SunWordWithCount value) throws Exception {
		writerFile(value.toString());
	}

	private void writerFile(String s) throws IOException {
		Path logFile = Paths.get("./LearnFlink/src/main/resources/wordcount.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
