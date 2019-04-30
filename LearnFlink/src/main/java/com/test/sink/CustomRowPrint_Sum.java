package com.test.sink;

import com.test.util.DataUtil;
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
import java.util.Map;

public class CustomRowPrint_Sum extends RichSinkFunction<Row> {
	private String fileName;
	private Counter sum = null;
	private Map<Long,Integer> map;
	public CustomRowPrint_Sum(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("customSum");
		sum = metricGroup.counter("sum");
		map = DataUtil.getStatisticsData();
		super.open(parameters);
	}

	@Override
	public void invoke(Row value) throws Exception {
		StringBuilder stringBuilder = new StringBuilder();
		Timestamp timestamp = (Timestamp) value.getField(1);
		stringBuilder.append(value.toString());
		sum.inc((Long) value.getField(0));
		if (map.containsKey(timestamp.getTime())){
			stringBuilder
				.append("————————")
				.append(map.get(timestamp.getTime()));
		}
		stringBuilder
			.append("---sum---")
			.append(sum.getCount());
		writerFile(stringBuilder.toString());

	}
	public static synchronized void writerFile(String s) throws IOException {
		Path logFile = Paths.get( URLUtil.baseUrl + "test.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
