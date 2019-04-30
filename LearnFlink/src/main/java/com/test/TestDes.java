package com.test;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.io.IOException;

public class TestDes {
	public static void main(String[] args) {
//		testMethod1();
		testMethod3();
	}
	public static void testMethod1(){
		JSONDeserializationSchema jsonDeserializationSchema = new JSONDeserializationSchema();
		String message = "{\"appId\":99}";
		try {
			ObjectNode objectNode = jsonDeserializationSchema.deserialize(message.getBytes());
			System.out.println(objectNode.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void testMethod2(){
		JSONKeyValueDeserializationSchema jsonKeyValueDeserializationSchema = new JSONKeyValueDeserializationSchema(false);
		String message = "{\"appId\":99}";

	}

	public static void testMethod3(){
		String message = "{\"appId\":99,\"total\":0.116,\"totalExclusive\":0.117}";
		TableSchemaBuilder tableSchemaBuilder=TableSchema.builder();
		tableSchemaBuilder.field("id", Types.STRING())
			.field("totalExclusive",Types.FLOAT())
			.field("appId",Types.INT())
			.field("total",Types.FLOAT());
		JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema(tableSchemaBuilder.build().toRowType());
		try {
			Row row = jsonRowDeserializationSchema.deserialize(message.getBytes());
			for (int i = 0; i < row.getArity(); i++) {
				System.out.println(row.getField(i));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
