package com.test.customTableSource;

import com.google.gson.*;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;

public class RowDeserializer implements JsonDeserializer<Row> {



	@Override
	public Row deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        Row row = new Row(3);
        row.setField(0,jsonObject.get("a").getAsString());
		row.setField(1,jsonObject.get("b").getAsInt());
		row.setField(2,jsonObject.get("rtime").getAsLong());
		return row;
	}
}
