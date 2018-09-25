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
import java.util.ArrayList;
import java.util.List;

public class CustomBatchSink extends RichOutputFormat<Row> {
	private List<Row> list;

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		list = new ArrayList<>();
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		list.add(record);
		if (record != null){
			writerFile(record.toString());
		}
	}

	public void writerFile(String s) throws IOException {
		Path logFile = Paths.get("./LearnFlink/src/main/resources/" + "test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}

	@Override
	public void close() throws IOException {
		System.out.println(list.size());
		System.out.println("dddddddddddddddddddddd");
	}
}
