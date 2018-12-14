package com.test.cache;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Value {
	private Object value;
	private TypeInformation typeInformation;

	public Value(Object value, TypeInformation typeInformation) {
		this.value = value;
		this.typeInformation = typeInformation;
	}

	public Object getValue() {
		return value;
	}

	public TypeInformation getTypeInformation() {
		return typeInformation;
	}

}
