package com.test.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class TestEventWriter {
	private static Gson gson = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
	public static void main(String[] args) {
		long time =  0L;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			time = sdf.parse("2018-09-20 1:33:00").getTime();
			testMethod1(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(long time){
		String[] userName = {"a","b","c","d","e","f","g"};
		Path logFile = Paths.get("./LearnFlink/src/main/resources/testEvent.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			for (int i = 0; i < 10000; i++) {
				Map<String,Object> map = new HashMap<>();
				map.put("a",userName[RandomUtil.getRandom(1)]);
				if (i < 10){
					map.put("b",100);
				}else if (i < 20){
					map.put("b",50);
				}else if (i < 30){
					map.put("b",100);
				}else{
					map.put("b",50);
				}
//                map.put("user_count",getRandom(4) + 1);
				map.put("time",time);
				time = time + 60000;
				map.put("c",11.0D);
				String s = gson.toJson(map);
				writer.newLine();
				writer.write(s);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
