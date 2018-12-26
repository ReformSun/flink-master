package com.test.gson;

import com.google.gson.*;
import model.Event;

import java.lang.reflect.Type;

public class EventDeserializer implements JsonDeserializer<Event> {
	@Override
	public Event deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		return new Event(jsonObject.get("b").getAsInt(),jsonObject.get("a").getAsString(),jsonObject.get("c").getAsDouble(),jsonObject.get("time").getAsLong());
	}
}
