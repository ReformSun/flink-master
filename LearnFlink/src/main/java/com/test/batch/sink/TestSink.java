package com.test.batch.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestSink extends RichOutputFormat<Row> {
	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

	}

	@Override
	public void writeRecord(Row record) throws IOException {
		writerFile(record.toString());
	}

	@Override
	public void close() throws IOException {

	}

	public static void writerFile(String s) throws IOException {
		Path logFile = Paths.get(".\\src\\main\\resource\\test1.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
