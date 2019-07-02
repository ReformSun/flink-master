package com.test.sink;

import com.test.util.URLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;

public class CustomRowPrint extends RichSinkFunction<Row> {
	private String fileName;
	private Counter sum = null;
	public CustomRowPrint(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
//		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("customSum");
//		sum = metricGroup.counter("sum");
		super.open(parameters);
	}

	@Override
	public void invoke(Row value) throws Exception {
//		Timestamp timestamp = (Timestamp) value.getField(1);
		writerFile(value.toString(),fileName);
	}
	public static synchronized void writerFile(String s,String fileName) throws IOException {
		if (fileName == null){
			fileName = "test.txt";
		}
		Path logFile = Paths.get( URLUtil.baseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
