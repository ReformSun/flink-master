package com.test.filesource;

import com.test.util.URLUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileSource implements SourceFunction<String> {
	private String path;
	private DeserializationSchema<Row> deserializationS;

	public FileSource() {
		path = URLUtil.baseUrl + "source.txt";
	}
	public FileSource(String path) {
		this.path = path;
	}

	public FileSource(String path, DeserializationSchema<Row> deserializationS) {
		this.path = path;
		this.deserializationS = deserializationS;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null){
				ctx.collect(line);
			}
		}
	}
	@Override
	public void cancel() {

	}
}
