package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple3 extends RichSinkFunction<Tuple3> {
	@Override
	public void invoke(Tuple3 value) throws Exception {
		java.nio.file.Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
