package com.test.flatMap_1;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import test.TimeAndNumber;

public class FlatMapFunctionTimeAndNumber implements FlatMapFunction<String, TimeAndNumber> {
	private static JsonParser jsonParser = new JsonParser();
	@Override
	public void flatMap(String value, Collector<TimeAndNumber> out) throws Exception {
		JsonElement jsonElement = jsonParser.parse(value);
		Long timestamp = jsonElement.getAsJsonObject().get("timestamp").getAsLong();
		Long number = jsonElement.getAsJsonObject().get("number").getAsLong();
		out.collect(new TimeAndNumber(timestamp,number));
	}
}
