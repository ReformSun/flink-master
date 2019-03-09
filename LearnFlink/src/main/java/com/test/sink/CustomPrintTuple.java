package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple<T extends Tuple> extends RichSinkFunction<T> {
	private String fileName;

	public CustomPrintTuple(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void invoke(T value) throws Exception {
		Path logFile = Paths.get("./LearnFlink/src/main/resources/" + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
