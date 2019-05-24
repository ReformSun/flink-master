package com.test.customInfluxdb;

import com.test.util.RandomUtil;
import com.test.util.TimeUtil;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class TestInfluxdb2 {

	private static final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();
	public static void main(String[] args) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		InfluxDBSinkBase influxDBSinkBase = new InfluxDBSinkBase("http://172.31.24.36:8086","","","");
		InfluxDB influxDB = InfluxDBFactory.connect("http://172.31.24.36:8086", "root","");

//		influxDB.createDatabase("testDB");
		testMethod1(influxDB,simpleDateFormat);
		testMethod2(influxDB,simpleDateFormat);
	}

	public  static void enableBatch(InfluxDB influxDB){
		int actions = 1000;
		int flushDuration = 30;
		TimeUnit flushDurationTimeUnit = TimeUnit.SECONDS;
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		BiConsumer<Iterable<Point>, Throwable> exceptionHandler = new BiConsumer<Iterable<Point>, Throwable>() {
			@Override
			public void accept(Iterable<Point> points, Throwable throwable) {
				failureThrowable.getAndSet(throwable);
			}
		};

		influxDB.enableBatch(actions,flushDuration,flushDurationTimeUnit,threadFactory,exceptionHandler);
	}

	// 单点插入
	public static void testMethod1(InfluxDB influxDB, SimpleDateFormat simpleDateFormat) {
		String[] types = {"1","2"};
		long time = TimeUtil.toLong("2019-05-23 1:33:00:000");
		for (int i = 0; i < 10; i++) {
			Point.Builder builder = Point.measurement("stats_jvm_gc_instantaneous");
			Map<String, String> tags = new HashMap<>();
			Map<String, Object> fields = new HashMap<>();
			fields.put("gc_count",10);
			fields.put("gc_times",1111);
			fields.put("time",time + 60000);
			tags.put("gc_type",types[RandomUtil.getRandom(2)]);
			tags.put("metric_name", "FullGC");
			tags.put("server_id","dddddddddd");
			tags.put("host", "localhost");
			tags.put("app_id","1");
			builder.time(time + 60000,TimeUnit.MILLISECONDS);
			builder.tag(tags);
			builder.fields(fields);
			influxDB.write("testDB", "", builder.build());
			influxDB.close();
		}
	}

	public static void testMethod2(InfluxDB influxDB, SimpleDateFormat simpleDateFormat){
		String[] types = {"1","2","3","4"};
		long time = TimeUtil.toLong("2019-05-23 1:33:00:000");
		for (int i = 0; i < 10; i++) {
			Point.Builder builder = Point.measurement("stats_server_physical_memory");
			Map<String, String> tags = new HashMap<>();
			Map<String, Object> fields = new HashMap<>();
			fields.put("memory_count",10);
			fields.put("memory_size",1111);
			fields.put("time",time + 60000);
			tags.put("memory_type",types[RandomUtil.getRandom(4)]);
			tags.put("metric_name", "FullGC");
			tags.put("server_id","dddddddddd");
			tags.put("host", "localhost");
			tags.put("app_id","1");
			builder.time(time + 60000,TimeUnit.MILLISECONDS);
			builder.tag(tags);
			builder.fields(fields);
			influxDB.write("testDB", "", builder.build());
			influxDB.close();
		}
	}


}
