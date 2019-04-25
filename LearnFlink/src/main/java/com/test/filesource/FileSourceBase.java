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
	private long interval = 0;

	public FileSourceBase(DeserializationSchema<T> deserializationS, String path,long interval) {
		this.deserializationS = deserializationS;
		this.path = path;
		this.interval = interval;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null && !line.equals("")){
				T row = deserializationS.deserialize(line.getBytes());
				ctx.collect(row);
				if (interval > 0){
					Thread.sleep(interval);
				}
			}
		}
	}

	@Override
	public void cancel() {

	}
}
