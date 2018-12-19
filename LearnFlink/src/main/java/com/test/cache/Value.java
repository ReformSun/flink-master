package com.test.cache;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Value {
	private Object value;
	private String typeInformation;

	public Value(Object value, String typeInformation) {
		this.value = value;
		this.typeInformation = typeInformation;
	}

	public Object getValue() {
		switch (typeInformation){
			case "string" : return value instanceof String ? value : String.valueOf(value);
			case "long": return value instanceof Long ? value : Long.valueOf(String.valueOf(value));
			case "date": return value instanceof Long ? value : Long.valueOf(String.valueOf(value));
			case "double" : return value instanceof Double ? value : Double.valueOf(String.valueOf(value));
			default:return value;
		}
	}
	public String getTypeInformation() {
		return typeInformation;
	}

}
