package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple2<T,F> extends RichSinkFunction<Tuple2<T,F>> {
	public CustomPrintTuple2() {
	}

	@Override
	public void invoke(Tuple2 value) throws Exception {
		java.nio.file.Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
