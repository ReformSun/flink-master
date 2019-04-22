package com.test.learnWindows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.OutputTag;

import java.util.List;

public class TestOutPutTag extends AbstractTestMain11{
	public static void main(String[] args) {
		try{
			testMethod1();
//			testMethod2();
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		List<Tuple2<String,Long>> list = getTestMain12data();
		DataStreamSource<Tuple2<String,Long>> dataStreamSource = env.fromCollection(list);
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(Types.STRING(),Types.LONG());
		OutputTag<Tuple2<String,Long>> outputTag = new OutputTag("aa",tupleTypeInfo);
		dataStreamSource.getSideOutput(outputTag).map(new MapFunction<Tuple2<String,Long>, String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("aaaaaaa");
				return "cccc";
			}
		}).print();
		OutputTag<Tuple2<String,Long>> outputTag1 = new OutputTag("cc",tupleTypeInfo);
		dataStreamSource.getSideOutput(outputTag1).map(new MapFunction<Tuple2<String,Long>, String>() {
			@Override
			public String map(Tuple2<String, Long> value) throws Exception {
				System.out.println("cccccc");
				return "cccc";
			}
		}).print();
	}
}
