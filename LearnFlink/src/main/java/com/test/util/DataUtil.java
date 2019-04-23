package com.test.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import model.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import static com.test.util.RandomUtil.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

	/**
	 * @param a 第一批数据的数量
	 * @param b 第二批数据的数量
	 * @param num 总的数据量
	 * @return
	 */
	public static List<Tuple3<String,Integer,Long>> getTuple3_Int_timetamp(int a,int b,int num){
		List<Tuple3<String,Integer,Long>> list = new ArrayList<>();
		Long date = 1534472000050L;

		for (int i = 0; i < num; i++) {
			if (i < a){
				date = date + 20010;
				Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date);
				list.add(tuple3);
			}else if (i < a + b){
				date = date + 10010;
				Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(98),i + 1,date);
				list.add(tuple3);
			}else {
				date = date + 10010;
				Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(99),i + 1,date);
				list.add(tuple3);
			}
		}
		return list;
	}

	/**
	 * 有序的时间顺序
	 */
	public static List<Tuple3<String,Integer,Long>> getTuple3_Int_timetamp_OrderedTime_1M(int num){
		List<Tuple3<String,Integer,Long>> list = new ArrayList<>();
		Long date = 1534472000000L;
		Long endDate  = date + 60000;

		list.add(new Tuple3(getStringFromInt(97), 1,date));
		for (int i = 0; i < num - 2; i++) {
			date = date + 60000/num;
			Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date);
			list.add(tuple3);
		}
		Tuple3<String,Integer,Long> tuple = new Tuple3(getStringFromInt(97), 1,endDate);
		list.add(tuple);
		return list;
	}

	/**
	 * Time out of order 时间乱序
	 * 一分钟的乱序时间
	 */
	public static List<Tuple3<String,Integer,Long>> getTuple3_Int_timetamp_timeOutOfOrder_1M(int num){
		List<Tuple3<String,Integer,Long>> list = new ArrayList<>();
		Long date = 1534472000000L;

		list.add(new Tuple3(getStringFromInt(97), 1,date));
		for (int i = 0; i < num - 2; i++) {
			Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date + getRandom(1,50000));
			list.add(tuple3);
		}
		Tuple3<String,Integer,Long> tuple = new Tuple3(getStringFromInt(97), 1,date + 60000);
		list.add(tuple);
		return list;
	}

	/**
	 * 设置两分钟的无序时间事件
	 * @param num
	 * @return
	 */
	public static List<Tuple3<String,Integer,Long>> getTuple3_Int_timetamp_timeOutOfOrder_2M(int num){
		List<Tuple3<String,Integer,Long>> list = new ArrayList<>();
		Long date = 1534472000000L;

		list.add(new Tuple3(getStringFromInt(97), 1,date));
		for (int i = 0; i < num - 2; i++) {
			Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date + getRandom(1,50000));
			list.add(tuple3);
		}
		Tuple3<String,Integer,Long> tuple = new Tuple3(getStringFromInt(97), 1,date + 61000);
		list.add(tuple);
		list.add(new Tuple3(getStringFromInt(97), 100,date + getRandom(1,50000)));

		date = date + 62000;
		for (int i = 0; i < num - 2; i++) {
			Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date + getRandom(1,50000));
			list.add(tuple3);
		}
		list.add(new Tuple3(getStringFromInt(97), 1,date + 61000));


		return list;
	}

	/**
	 * @param a 从0到a 创造的数据
	 * @param b 从a到b 创造的数据
	 * @param num 总的数据
	 * @return
	 */
	public static List<Event> getEventCollection(int a, int b, int num) {
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, date);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(60, "a", 2.0, date);
				inputEvents.add(event);
			} else {
				date = date + 10000;
				Event event = new Event(i, "c", 3.0, date);
				inputEvents.add(event);
			}
		}
		return inputEvents;
	}

	public static List<Event> getEventCollection2() {
		int a = 1;int b = 2;int num = 4;
		List<Event> inputEvents = new ArrayList<>(); // 构建数据源
		Long date = 1534472000000L;
		for (int i = 0; i < num; i++) {
			if ( i < a) {
				date = date + 20000;
				Event event = new Event(50, "a", i, date);
				inputEvents.add(event);
			} else if (i < b) {
				date = date + 10000;
				Event event = new Event(60, "b", 2.0, date);
				inputEvents.add(event);
			} else {
				date = date + 10000;
				Event event = new Event(70, "c", 3.0, date);
				inputEvents.add(event);
			}
		}
		return inputEvents;
	}

	// 把文件中定义的字符串转换数据格式
	public static List<Tuple3<String,Integer,Long>> getListFromFile(String fileName){
		try {
			if (fileName == null)fileName = "dataTestFile.txt";
			List<Tuple3<String,Integer,Long>> list0 = new ArrayList<>();
			List<String> list = FileReader.readFile(URLUtil.baseUrl + fileName);
			Iterator<String> iterator = list.iterator();
			while (iterator.hasNext()){
				String s = iterator.next();
				List<String> list2 = Splitter.on(",").trimResults(CharMatcher.is('(').or(CharMatcher.is(')'))).splitToList(s);
				if (list2.size() == 3){
					Tuple3<String,Integer,Long> tuple3 = new Tuple3<>();
					tuple3.f0 = list2.get(0);
					tuple3.f1 = Integer.valueOf(list2.get(1));
					tuple3.f2 = Long.valueOf(list2.get(2));
					list0.add(tuple3);
				}
			}
			return list0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	// 把文件中定义的字符串转换数据格式
	public static List<Map<String,Object>> getList_MapFromFile(String fileName){
		try {
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.registerTypeAdapter(Map.class,new MapJsonDeserializer());
			Gson gson = gsonBuilder.create();
			if (fileName == null)fileName = "dataTestTableFile.txt";
			List<Map<String,Object>> list0 = new ArrayList<>();
			List<String> list = FileReader.readFile(URLUtil.baseUrl + fileName);
			Iterator<String> iterator = list.iterator();
			while (iterator.hasNext()){
				String s = iterator.next();
				if (!s.equals("")){
					Map<String,Object> map = gson.fromJson(s, Map.class);
					list0.add(map);
				}else {
					break;
				}
			}
			return list0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取指定字符
	 * @param number 字符ascII码
	 * @return
	 */
	public static String getStringFromInt(int number)
	{
		char[] chars = {(char)(number)};
		return new String(chars);
	}


	public static void main(String[] args) {
//		System.out.println(getStringFromInt(97));
//		Iterator<Tuple3<String,Integer,Long>> iterator = getListFromFile(null).iterator();
//		while (iterator.hasNext()){
//			System.out.println(iterator.next().toString());
//		}

	}
}
