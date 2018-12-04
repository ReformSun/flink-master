package com.test.customInfluxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.Point.Builder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class TestInfluxdb {

	private static final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();
	public static void main(String[] args) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		InfluxDBSinkBase influxDBSinkBase = new InfluxDBSinkBase("http://172.31.24.36:8086","","","");
		InfluxDB influxDB = InfluxDBFactory.connect("http://172.31.24.36:8086", "root","");

//		influxDB.createDatabase("testDB");
//		testMethod1(influxDB,simpleDateFormat);
		testMethod2(influxDB,simpleDateFormat);
//		testMethod3(influxDB,simpleDateFormat);
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
		Point.Builder builder = Point.measurement("testtable");
		Map<String, String> tags = new HashMap<>();
		Map<String, Object> fields = new HashMap<>();
		tags.put("TAG_NAME","ddddcccssdsdds");
		fields.put("TAG_VALUE","cccccssssssdfdfsfs");
		fields.put("TIMAMPEST", simpleDateFormat.format(new Date()));
		builder.tag(tags);
		builder.fields(fields);
		influxDB.write("testDB", "", builder.build());
		influxDB.close();
	}
	// 批插入
	public static void testMethod3(InfluxDB influxDB, SimpleDateFormat simpleDateFormat) {
		BatchPoints.Builder batchBuilder = BatchPoints.database("testDB");
		Point.Builder builder = Point.measurement("testtable");
		Map<String, String> tags = new HashMap<>();
		Map<String, Object> fields = new HashMap<>();
		tags.put("TAG_NAME","dddd");
		fields.put("TAG_VALUE","ccccc");
		fields.put("TIMAMPEST", simpleDateFormat.format(new Date()));
		builder.tag(tags);
		builder.fields(fields);
		batchBuilder.point(builder.build());

		Point.Builder builder2 = Point.measurement("testtable");
		Map<String, String> tags2 = new HashMap<>();
		Map<String, Object> fields2 = new HashMap<>();
		tags2.put("TAG_NAME","ddddss");
		fields2.put("TAG_VALUE","ccdd");
		fields2.put("TIMAMPEST", simpleDateFormat.format(new Date()));
		builder2.tag(tags2);
		builder2.fields(fields2);
		batchBuilder.point(builder2.build());


       try {
		   influxDB.write(batchBuilder.build());
	   }catch (Exception e){

	   }

	}

	public static void testMethod2(InfluxDB influxDB,SimpleDateFormat simpleDateFormat) {
		enableBatch(influxDB);
		String command = "insert testtab,TAG_NAME=cc TAG_VALUE=ss " + simpleDateFormat.format(new Date());
//		command = "SELECT * FROM \"testtable\" WHERE time > now() - 5m";
		influxDB.query(new Query(command,"testDB"));
//		try {
//			influxDB.query(new Query(command,"testDB"));
//		}catch (Exception e){
//			System.out.println(e);
//		}
	}
}
