package com.test.customTableSource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;

public class ServerTask implements Runnable{
	private Socket socket;
	private SourceFunction.SourceContext<Row> sourceContext;
	private final DeserializationSchema<Row> rowDeserializationSchema;

	public ServerTask(Socket socket, SourceFunction.SourceContext<Row> sourceContext, DeserializationSchema<Row> rowDeserializationSchema) {
		this.socket = socket;
		this.sourceContext = sourceContext;
		this.rowDeserializationSchema = rowDeserializationSchema;
	}

	@Override
	public void run() {
		try {
			//调用与客户端实现的方法
			handleSocket();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void handleSocket() {
		BufferedReader br = null;
		Writer writer = null;
		try {
			//接收客户端传递过来的参数，设置接收的编码格式为：GBK
			br = new BufferedReader(new InputStreamReader(
				socket.getInputStream(), "utf-8"));
			StringBuilder sb = new StringBuilder();
			String temp;
			int index;
			while ((temp = br.readLine()) != null) {
				sourceContext.collect(rowDeserializationSchema.deserialize(temp.getBytes()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if (socket != null) {
					socket.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
