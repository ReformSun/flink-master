package com.test.customInfluxdb;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.util.Preconditions;

public class InfluxDBSinkBaseBuilder {
	private  String openurl;
	private  String username;
	private  String password;
	private  String database;//数据库
	private  String measurement;
	private boolean flushOnCheckpoint;

	public InfluxDBSinkBaseBuilder setOpenurl(String openurl) {
		this.openurl = openurl;
		return this;
	}

	public InfluxDBSinkBaseBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	public InfluxDBSinkBaseBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	public InfluxDBSinkBaseBuilder setDatabase(String database) {
		this.database = database;
		return this;
	}

	public InfluxDBSinkBaseBuilder setMeasurement(String measurement) {
		this.measurement = measurement;
		return this;
	}

	public InfluxDBSinkBaseBuilder setFlushOnCheckpoint(boolean flushOnCheckpoint) {
		this.flushOnCheckpoint = flushOnCheckpoint;
		return this;
	}

	public InfluxDBSinkBase build(){
		Preconditions.checkNotNull(openurl,"url 不能为null");
		Preconditions.checkNotNull(username,"用户名不能为null");
		Preconditions.checkNotNull(database,"数据库名不能为null");
		Preconditions.checkNotNull(measurement,"表名不能为null");
		return new InfluxDBSinkBase(openurl,username,password,database,measurement);
	}
}
