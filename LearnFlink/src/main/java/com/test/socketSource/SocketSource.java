package com.test.socketSource;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.test.customTableSource.RowDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketSource implements SourceFunction<String> {
	private final int port = 9000;
	private final String hostName = "localhost";
	private  ServerSocket server;
	public SocketSource() {
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		try {
			server = new ServerSocket(port);
			while (true) {
				//server尝试接收其他Socket的连接请求，server的accept方法是阻塞式的
				//服务器启动后，或完成一个连接后，会再次在此等候下一个连接
				Socket socket = server.accept();
				// 每接收到一个Socket就建立一个新的线程来处理它
				new Thread(new ServerTask(socket,ctx)).start();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void cancel() {
		try {
			server.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
