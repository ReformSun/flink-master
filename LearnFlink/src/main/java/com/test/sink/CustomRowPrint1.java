package com.test.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomRowPrint1 extends RichSinkFunction<Tuple2<Boolean, Row>> {
	private String fileName;

	public CustomRowPrint1(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> tuple2) throws Exception {
		writerFile(tuple2.getField(1).toString());
	}
	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get("./LearnFlink/src/main/resources/" + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
