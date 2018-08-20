package com.test.socket;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class TestMain {
	public static void main(String[] args) {
		Socket client = null;
		Writer writer = null;

		String host = "127.0.0.1"; // 要连接的服务端IP地址
		int port = 9000; // 要连接的服务端对应的监听端口
		try {
			// 与服务端建立连接
			client = new Socket(host, port);
			// 建立连接后就可以往服务端写数据了
			// 客户端发送给服务端的参数，设置传递参数的编码格式为：GBK

			writer = new OutputStreamWriter(client.getOutputStream(), "utf-8");
			long time = 1534600170000L;
			for (int i = 60; i < 70; i++) {
				time = time + i * 10000;
				writer.write("{\"username\":\"小张\",\"countt\":2,\"rtime\":" + time + "}");
				writer.write("\n");//BufferedReader读取，记得加换行
				writer.flush();
				Thread.sleep(1000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				if (writer != null) {
					writer.close();
				}
				if (writer != null) {
					client.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
