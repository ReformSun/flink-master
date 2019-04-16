package com.test.learnWindows;

import com.google.gson.Gson;
import com.test.socketSource.SocketSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractTestMain1 {
	public static final Gson gson = new Gson();
	public static final StreamExecutionEnvironment env;
	static {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	}

	public static DataStreamSource<String> getInput(){
		DataStreamSource<String> input =  env.addSource(KafkaUtil.getKafkaConsumer09Source("ddddddd")).setParallelism(1);
		return input;
	}
	public static DataStreamSource<String> getInput(String topic){
		DataStreamSource<String> input =  env.addSource(KafkaUtil.getKafkaConsumer010Source(topic)).setParallelism(1);
		return input;
	}
	public static DataStreamSource<String> getInput2(){
		DataStreamSource<String> input2 =  env.addSource(KafkaUtil.getKafkaConsumer09Source("ccccccc")).setParallelism(1);
		return input2;
	}

	public static DataStreamSource<String> getInputSocket(int port){
		DataStreamSource<String> input = null;
		if (port == 0){
			input = env.addSource(new SocketSource());
		}else {
			input = env.addSource(new SocketSource(port));
		}
		return input;
	}

	public static String getRandom()
	{
		Random random = new Random();
		int number = random.nextInt(26) + 97;
		char[] chars = {(char)(number)};
		return new String(chars);
	}

	public static String getStringFromInt(int number)
	{
		char[] chars = {(char)(number)};
		return new String(chars);
	}

	public static List<Tuple3<String,Integer,Long>> getTestdata(){
		List<Tuple3<String,Integer,Long>> list = new ArrayList<>();
		Long date = 1534472000050L;

		for (int i = 0; i < 5; i++) {

			if (i < 2){
				date = date + 20010;
				Tuple3<String,Integer,Long> tuple3 = new Tuple3(getStringFromInt(97),i + 1,date);
				list.add(tuple3);
			}else if (i < 4){
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



}
