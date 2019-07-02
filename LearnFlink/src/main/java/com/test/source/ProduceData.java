package com.test.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;

public interface ProduceData<T> extends Serializable{
	public T getData();
	public TypeInformation<T> getProducedType();
}
