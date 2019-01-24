package com.test.jsonFotmats;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * 反序列化
 */
public class TestJsonDs {
	public static void main(String[] args) {
		testMethod1();
	}

	public static void testMethod1() {
		String[] fields = {"aa","cc"};
		TypeInformation<Row> typeInformations = Types.ROW_NAMED(fields,Types.STRING,Types.INT);
		JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema(typeInformations);
		String s = "{\"aa\":\"aa\",\"cc\":1}";
		try {
			Row row = jsonRowDeserializationSchema.deserialize(s.getBytes());
			System.out.println(row.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
