package com.test.learnMetric.group;

import org.apache.flink.metrics.*;

import java.util.Map;

public class TestMetricGroup implements MetricGroup{
	@Override
	public Counter counter(int name) {
		return null;
	}

	@Override
	public Counter counter(String name) {
		return null;
	}

	@Override
	public <C extends Counter> C counter(int name, C counter) {
		return null;
	}

	@Override
	public <C extends Counter> C counter(String name, C counter) {
		return null;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return null;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		return null;
	}

	@Override
	public <H extends Histogram> H histogram(String name, H histogram) {
		return null;
	}

	@Override
	public <H extends Histogram> H histogram(int name, H histogram) {
		return null;
	}

	@Override
	public <M extends Meter> M meter(String name, M meter) {
		return null;
	}

	@Override
	public <M extends Meter> M meter(int name, M meter) {
		return null;
	}

	@Override
	public MetricGroup addGroup(int name) {
		return null;
	}

	@Override
	public MetricGroup addGroup(String name) {
		return null;
	}

	@Override
	public MetricGroup addGroup(String key, String value) {
		return null;
	}

	@Override
	public String[] getScopeComponents() {
		return new String[0];
	}

	@Override
	public Map<String, String> getAllVariables() {
		return null;
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return null;
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return null;
	}
}
