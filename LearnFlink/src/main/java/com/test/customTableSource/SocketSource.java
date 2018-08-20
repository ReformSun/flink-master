package com.test.customTableSource;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketSource implements SourceFunction<Row> {
	private final int port = 9000;
	private final String hostName = "localhost";
	private static final Socket socket = new Socket();
	private static final Gson gson;
	private final DeserializationSchema<Row> rowDeserializationSchema;

	static {
		GsonBuilder gsonBuilder = new GsonBuilder();
		RowDeserializer dateDeserializer = new RowDeserializer();
		gsonBuilder.registerTypeAdapter(Row.class,dateDeserializer);
		gson = gsonBuilder.create();
	}

	public SocketSource(DeserializationSchema<Row> rowDeserializationSchema) {
		this.rowDeserializationSchema = rowDeserializationSchema;
	}

	@Override
	public void run(SourceContext<Row> ctx) throws Exception {

		// 为了简单起见，所有的异常信息都往外抛
		int port = 9000;
		// 定义一个ServerSocket监听在端口8899上
		try {
			ServerSocket server = new ServerSocket(port);
			while (true) {
				//server尝试接收其他Socket的连接请求，server的accept方法是阻塞式的
				//服务器启动后，或完成一个连接后，会再次在此等候下一个连接
				Socket socket = server.accept();
				// 每接收到一个Socket就建立一个新的线程来处理它
				new Thread(new ServerTask(socket,ctx,rowDeserializationSchema)).start();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void cancel() {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
