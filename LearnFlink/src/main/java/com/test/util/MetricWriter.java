package com.test.util;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MetricWriter {
	public static void writerFile(Counter metric, String fileName) throws IOException {
		Path logFile = Paths.get(URLUtil.metricBaseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(metric.getCount() + "");
		}
	}
}
