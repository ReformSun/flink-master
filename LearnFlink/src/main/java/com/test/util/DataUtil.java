package com.test.util;

import model.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class DataUtil {
	public static List<Tuple2<Long,String>> getTuple2(){
		List<Tuple2<Long,String>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,String> tuple2 = new Tuple2(date,"aa");
			list.add(tuple2);
		}
		return list;
	}

	public static List<Tuple2<Long,Boolean>> getTuple2_Boolean(){
		List<Tuple2<Long,Boolean>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,Boolean> tuple2 = new Tuple2(date,true);
			list.add(tuple2);
		}
		return list;
	}
	public static List<Tuple2<Long,Integer>> getTuple2_Int(){
		List<Tuple2<Long,Integer>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple2<Long,Integer> tuple2 = new Tuple2(date,i);
			list.add(tuple2);
		}
		return list;
	}

	public static List<Tuple3<Long,Integer,String>> getTuple3_Int(){
		List<Tuple3<Long,Integer,String>> list = new ArrayList<>();
		Long date = 1534472000000L;
		for (int i = 0; i < 10; i++) {
			date = date + 60000;
			Tuple3<Long,Integer,String> tuple3 = new Tuple3(date,i,"aa");
			list.add(tuple3);
		}
		return list;
	}
}
