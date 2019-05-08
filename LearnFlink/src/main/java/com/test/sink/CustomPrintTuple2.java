package com.test.sink;

import com.test.util.URLUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple2<T,F> extends RichSinkFunction<Tuple2<T,F>> {
	private String fileName = "test1.txt";

	public CustomPrintTuple2() {
	}

	public CustomPrintTuple2(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void invoke(Tuple2<T,F> value, Context context) throws Exception {
		Path logFile = Paths.get(URLUtil.baseUrl  + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(value.toString());
		}
	}
}
