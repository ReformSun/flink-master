package com.test.cache;

public enum CommonValue {
//	数据库默认值
	HOST("localhost"),
	PORT("5432"),
	DATABASE("testdb"),
	USERNAME("root"),
	PASSWORD("password"),
	CONNECTTIMEOUT(3),
// 数据库配置map中的key值
	HOST_KEY("host"),
	PORT_KEY("port"),
	DATABASE_KEY("database"),
	USERNAME_KEY("username"),
	PASSWORD_KEY("password"),
	TABLENAME_KEY("tablename"),
	CONNECTTIMEOUT_KEY("connectTimeout");



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
