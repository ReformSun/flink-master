package com.test.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class ProduceTuple3 implements ProduceData<Tuple3<String,Integer,Long>>{
	private int i = 0;
	@Override
	public Tuple3<String, Integer, Long> getData() {
		Tuple3<String,Integer,Long> tuple3;
		if (i % 2 == 0){
			tuple3 = new Tuple3<>("a",1,System.currentTimeMillis());
		}else {
			tuple3 = new Tuple3<>("b",1,System.currentTimeMillis());
		}
		i++;
		return tuple3;
	}

	@Override
	public TypeInformation<Tuple3<String, Integer, Long>> getProducedType() {

		return new TupleTypeInfo(Types.STRING,Types.INT,Types.LONG);
	}
}
