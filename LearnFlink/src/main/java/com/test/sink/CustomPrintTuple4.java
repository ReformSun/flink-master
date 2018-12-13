package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple4 extends RichSinkFunction<Tuple4> {
	private String path = "test.txt";

	public CustomPrintTuple4() {
	}
	public CustomPrintTuple4(String path) {
		this.path = path;
	}

	@Override
	public void invoke(Tuple4 value) throws Exception {
		java.nio.file.Path logFile = Paths.get("./LearnFlink/src/main/resources/" + path);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
