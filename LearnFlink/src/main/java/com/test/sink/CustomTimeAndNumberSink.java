package com.test.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import test.TimeAndNumber;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomTimeAndNumberSink extends RichSinkFunction<TimeAndNumber> {
	@Override
	public void invoke(TimeAndNumber value) throws Exception {
		java.nio.file.Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
