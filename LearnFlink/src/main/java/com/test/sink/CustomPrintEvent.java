package com.test.sink;

import com.test.util.URLUtil;
import model.Event;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintEvent extends RichSinkFunction<Event> {
	private String path = "test.txt";

	public CustomPrintEvent(String path) {
		this.path = path;
	}

	public CustomPrintEvent() {
	}

	@Override
	public void invoke(Event value) throws Exception {
		java.nio.file.Path logFile = Paths.get(URLUtil.baseUrl + path);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
