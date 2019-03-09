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
//			testMethod1(time);
			testMethod2(time);
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

	/**
	 * tid,pathhash,sid,pid,tm,cpathhash,st,du
	 * tid:业务唯一TraceID流水号
	 * pathhash:服务实例的唯一标识
	 * sid:被调用者的spanid
	 * pid:发起调用者的spanid
	 * tm:时间戳,已经存在
	 * cpathhash:调用者服务实例的唯一标识
	 * st:成功或失败说明 false true 1 0
	 * du:持续时间
	 */
	public static void testMethod2(long time){
		Path logFile = Paths.get("./LearnFlink/src/main/resources/sdn.csv");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			for (int i = 0; i < 10000; i++) {
				StringBuilder stringBuilder = new StringBuilder();
				// tid
				stringBuilder.append(RandomUtil.getRandomHash()).append(",");
				// pathhash
				stringBuilder.append(RandomUtil.getRandomHash()).append(",");
				// sid
				stringBuilder.append(RandomUtil.getRandomHash()).append(",");
				// pid
				stringBuilder.append(RandomUtil.getRandomHash()).append(",");
				// tm
				time = time + 60000;
				stringBuilder.append(time).append(",");
				// cpathhash
				stringBuilder.append(RandomUtil.getRandomHash()).append(",");
				// st
				stringBuilder.append(RandomUtil.getRandom(2)).append(",");
				// du
				stringBuilder.append(RandomUtil.getRandomHash());


				writer.newLine();
				writer.write(stringBuilder.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
