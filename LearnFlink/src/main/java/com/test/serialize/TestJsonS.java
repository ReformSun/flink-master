package com.test.serialize;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;

public class TestJsonS {
	public static void main(String[] args) {
		testMethod2();
	}

	public static void testMethod1() {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.INT,Types.SQL_TIMESTAMP);
		JsonRowSerializationSchema jsonRowSerializationSchema = new JsonRowSerializationSchema(rowTypeInfo);
	}

	public static void testMethod2() {
		TypeInformation[] typeInformations = {Types.INT,Types.STRING};
		String[] fieldNames = {"a","b"};
		TypeInformation<Row> rowSchema = new RowTypeInfo(typeInformations,fieldNames);
		JsonRowSerializationSchema jsonRowSerializationSchema = new JsonRowSerializationSchema(rowSchema);
		Row row = new Row(2);
		row.setField(0,1);
		row.setField(1,"ddd");

		byte[] bytes = jsonRowSerializationSchema.serialize(row);
		System.out.println(new String(bytes));
	}
}
