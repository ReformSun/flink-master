package com.test.cache;

import java.util.concurrent.TimeUnit;

public enum CommonValue {
	// common
	PASSWORD("password"),
	PASSWORD_KEY("password"),

	// postpre
//	数据库默认值
	HOST("localhost"),
	PORT("5432"),
	DATABASE("testdb"),
	USERNAME("root"),
	CONNECTTIMEOUT(3),
// 数据库配置map中的key值
	HOST_KEY("host"),
	PORT_KEY("port"),
	DATABASE_KEY("database"),
	USERNAME_KEY("username"),
	TABLENAME_KEY("tablename"),
	CONNECTTIMEOUT_KEY("connectTimeout"),
	// DataBase
	// 默认值
	DATABASE_URL("localhost"),
	USER("root"),
	DRIVER_CLASS("Driver_class"),
	// key值
	DATABASE_URL_KEY("url"),
	USER_KEY("user"),
	DRIVER_CLASS_KEY("driver_class"),
	// 缓存器配置信息
	// 默认值
	INTERVAL(5L),
	UNIT(TimeUnit.MILLISECONDS),
	// key值
	INTERVAL_KEY("refreshinterval"),
	UNIT_KEY("unit")
	;




	private Object value;
	CommonValue(Object value) {
		this.value = value;
	}
	public Object getValue() {
		return value;
	}

	public String getString(){
		return value instanceof String ? (String) value : value.toString();
	}

	public int getInt(){
		return value instanceof Integer ? (int) value : Integer.valueOf(((String)value));
	}
}
