package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple8 extends RichSinkFunction<Tuple8> {
	@Override
	public void invoke(Tuple8 value, Context context) throws Exception {
		java.nio.file.Path logFile = Paths.get("/Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
