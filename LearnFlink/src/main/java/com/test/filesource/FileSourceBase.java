package com.test.filesource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSourceBase<T> implements SourceFunction<T> {
	private DeserializationSchema<T> deserializationS;
	private String path;

	public FileSourceBase(DeserializationSchema<T> deserializationS, String path) {
		this.deserializationS = deserializationS;
		this.path = path;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null && !line.equals("")){
				T row = deserializationS.deserialize(line.getBytes());
				ctx.collect(row);
			}
		}
	}

	@Override
	public void cancel() {

	}
}
