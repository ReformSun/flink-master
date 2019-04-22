package com.test;

import com.test.keyby.KeySelectorTuple;
import com.test.keyby.KeySelectorTuple2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class TestTypeExtractor {
	public static void main(String[] args) {
//		testMethod1();
//		testMethod2();
		testMethod3();
	}

	public static void testMethod1(){
//		TypeInformation<?> typeInformation = TypeExtractor.createTypeInfo(String.class);
//		System.out.println(typeInformation.getTypeClass());
//		TypeInformation<?> typeInformation2 = TypeExtractor.createTypeInfo(Tuple2.class);
		TypeInformation<?> typeInformation2 = TypeExtractor.createTypeInfo(KeySelectorTuple2.class);
		System.out.println(typeInformation2.getTypeClass());

	}

	public static void testMethod2(){
		Tuple2<String,String> tuple2 = new Tuple2<>();
		KeySelectorTuple<Tuple2<String,String>,String> keySelectorTuple = new KeySelectorTuple<>(1);
		TypeInformation<Tuple2<String,String>> typeInformation = TypeExtractor.createTypeInfo((Class<Tuple2<String,String>>) tuple2.getClass());

		TypeExtractor.getKeySelectorTypes(keySelectorTuple,typeInformation);
	}

	public static void testMethod3(){
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(Types.STRING,Types.STRING);
	}


}
