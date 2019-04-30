package com.test.util;

import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class StreamExecutionEnvUtil {
	public static StreamExecutionEnvironment getStreamExecutionEnvironment(){
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return env;
	}

	public static StreamExecutionEnvironment getStreamExecutionEnvironment(Configuration conf){
		StreamExecutionEnvironment env;
		if (conf == null){
			env = StreamExecutionEnvironment.createLocalEnvironment(1,getConfiguration());
		}else {
			env = StreamExecutionEnvironment.createLocalEnvironment(1,conf);
		}
		return env;
	}

	public static Configuration getConfiguration(){
		return GlobalConfiguration.loadConfiguration("./LearnFlink/src/main/resources/");
	}


//	public static void main(String[] args) {
//		Configuration configuration = getConfiguration();
//		Map<String,String> map = configuration.toMap();
//		System.out.println(map.size());
//		System.out.println(configuration.toString());
//	}


}
