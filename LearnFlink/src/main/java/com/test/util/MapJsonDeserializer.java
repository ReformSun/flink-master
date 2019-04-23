package com.test.util;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class MapJsonDeserializer implements JsonDeserializer<Map> {
	@Override
	public Map deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		Map<String,Object> map = new HashMap<>();
		map.put("_sysTime",json.getAsJsonObject().get("_sysTime").getAsLong());
		map.put("user_name",json.getAsJsonObject().get("user_name").getAsString());
		map.put("user_count",json.getAsJsonObject().get("user_count").getAsInt());
		return map;
	}
}
