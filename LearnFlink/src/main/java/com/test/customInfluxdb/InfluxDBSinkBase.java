package com.test.customInfluxdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

import org.influxdb.dto.Query;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class InfluxDBSinkBase extends RichSinkFunction<Row> implements CheckpointedFunction {
	private  String openurl;
	private  String username;
	private  String password;
	private  String database;//数据库
	private  String measurement;//表名
	private InfluxDB influxDB;
	private boolean flushOnCheckpoint;
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public InfluxDBSinkBase(String openurl, String username, String password, String database,String measurement) {
		this.openurl = openurl;
		this.username = username;
		this.password = password;
		this.database = database;
		this.measurement = measurement;
	}

	@Override
	public void invoke(Row value) throws Exception {


	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if(influxDB == null){
			influxDB = InfluxDBFactory.connect(openurl, username, password);
			int actions = 1000;
			int flushDuration = 30;
			TimeUnit flushDurationTimeUnit = TimeUnit.SECONDS;
			ThreadFactory threadFactory = Executors.defaultThreadFactory();
			influxDB.enableBatch(actions,flushDuration,flushDurationTimeUnit,threadFactory,new BiConsumerImp());
			influxDB.createDatabase(database);
		}
		super.open(parameters);
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkErrorAndRethrow();
		if (flushOnCheckpoint) {
			influxDB.flush();
			checkErrorAndRethrow();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void close() throws Exception {
		influxDB.close();
		super.close();
	}
	private class BiConsumerImp implements BiConsumer<Iterable<Point>,Throwable> {
		@Override
		public void accept(Iterable<Point> points, Throwable throwable) {
			failureThrowable.getAndSet(throwable);
		}
	}

}
