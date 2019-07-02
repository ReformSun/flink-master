package com.test.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.net.Socket;

public class CustomSocketSink<T> extends RichSinkFunction<T>{
	private  Socket client = null;
	private  Writer writer = null;
	private String host = "10.4.251.99"; // 要连接的服务端IP地址
	private int port = 9000; // 要连接的服务端对应的监听端口

	public CustomSocketSink(String host) {
		if (host != null){
			this.host = host;
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		try {
			client = new Socket(host, port);
			writer = new OutputStreamWriter(client.getOutputStream(), "utf-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		super.open(parameters);
	}

	@Override
	public void invoke(T value) throws Exception {
		if (writer != null){
			writer.write(value.toString());
			writer.write("\n");
			writer.flush();
		}
	}

	@Override
	public void close() throws Exception {
		client.close();
		writer.close();
		super.close();
	}
}
