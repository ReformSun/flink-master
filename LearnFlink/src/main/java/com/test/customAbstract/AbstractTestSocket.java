package com.test.customAbstract;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.test.gson.EventDeserializer;
import com.test.learnWindows.KafkaUtil;
import com.test.socketSource.SocketSource;
import model.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Map;

public abstract class AbstractTestSocket extends AbstractTestCommon{
	public static Gson gson;
	static {
		GsonBuilder gsonBuilder = new GsonBuilder();
		EventDeserializer eventDeserializer = new EventDeserializer();
		gsonBuilder.registerTypeAdapter(Event.class,eventDeserializer);
		gson = gsonBuilder.create();
	}
	public static DataStream<Event> getInputFromSocket(){
		DataStreamSource<String> input =  env.addSource(new SocketSource()).setParallelism(1);
		DataStream<Event> dataStream = input.map(new RichMapFunction<String, Event>() {
			@Override
			public Event map(String value) throws Exception {
				return (Event) gson.fromJson(value,Event.class);
			}
		}).setParallelism(1);
		return dataStream;
	}
}
