package com.test.learnMetric.metricreporter;

import com.test.util.FileWriter;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.IOException;
import java.util.Map;

/**
 * {@link org.apache.flink.runtime.metrics.MetricRegistryImpl}
 */
public class FileMetricReporter extends AbstractReporter implements Scheduled {
	private static final String lineSeparator = System.lineSeparator();

	public FileMetricReporter() {
		System.out.println("");
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}

	@Override
	public void open(MetricConfig config) {

	}

	@Override
	public void close() {
		report();
	}

	@Override
	public void report() {
		reportCounter();
//		reportGauges();
//		reportMeters();
//		reportHistograms();
	}

	/**
	 * 只展示我自定义的测量值
	 */
	private void reportCounter(){
		StringBuilder builder = new StringBuilder();
		builder
			.append(lineSeparator)
			.append("-- Counters -------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Counter, String> metric : counters.entrySet()) {
//			if (metric.getValue().contains("custom")){
//				builder
//					.append(metric.getValue()).append(": ").append(metric.getKey().getCount())
//					.append(lineSeparator);
//			}
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getCount())
				.append(lineSeparator);
		}
		try {
			FileWriter.writerFile(builder.toString(), "metric/metricounters.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void reportGauges(){
		StringBuilder builder = new StringBuilder();
		builder
			.append(lineSeparator)
			.append("-- Gauges ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getValue())
				.append(lineSeparator);
		}
		try {
			FileWriter.writerFile(builder.toString(), "metric/metricgauges.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void reportMeters(){
		StringBuilder builder = new StringBuilder();
		builder
			.append(lineSeparator)
			.append("-- Meters ---------------------------------------------------------------------")
			.append(lineSeparator);
		for (Map.Entry<Meter, String> metric : meters.entrySet()) {
			builder
				.append(metric.getValue()).append(": ").append(metric.getKey().getRate())
				.append(lineSeparator);
		}
		try {
			FileWriter.writerFile(builder.toString(), "metric/metricmeters.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void reportHistograms(){
		StringBuilder builder = new StringBuilder();
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
		try {
			FileWriter.writerFile(builder.toString(), "metric/metrichistograms.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
