package com.test.learnMetric.metricreporter;

import com.test.util.FileWriter;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.IOException;
import java.util.Map;

public class FileMetricReporter extends AbstractReporter implements Scheduled {
	private static final String lineSeparator = System.lineSeparator();
	@Override
	public String filterCharacters(String input) {
		return input;
	}

	@Override
	public void open(MetricConfig config) {

	}

	@Override
	public void close() {

	}

	@Override
	public void report() {
		StringBuilder builder = new StringBuilder();
		builder
			.append(lineSeparator)
			.append("=========================== Starting metrics report ===========================")
			.append(lineSeparator);

		builder
			.append(lineSeparator)
			.append("-- Counters -------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Counter, String> metric : counters.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getCount())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Gauges ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getValue())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Meters ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Meter, String> metric : meters.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getRate())
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("-- Histograms -----------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
			HistogramStatistics stats = metric.getKey().getStatistics();
			builder
				.append(metric.getValue()).append(": count=").append(stats.size())
				.append(", min=").append(stats.getMin())
				.append(", max=").append(stats.getMax())
				.append(", mean=").append(stats.getMean())
				.append(", stddev=").append(stats.getStdDev())
				.append(", p50=").append(stats.getQuantile(0.50))
				.append(", p75=").append(stats.getQuantile(0.75))
				.append(", p95=").append(stats.getQuantile(0.95))
				.append(", p98=").append(stats.getQuantile(0.98))
				.append(", p99=").append(stats.getQuantile(0.99))
				.append(", p999=").append(stats.getQuantile(0.999))
				.append(lineSeparator);
		}

		builder
			.append(lineSeparator)
			.append("=========================== Finished metrics report ===========================")
			.append(lineSeparator);
		try {
			FileWriter.writerFile(builder.toString(),"metric.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
