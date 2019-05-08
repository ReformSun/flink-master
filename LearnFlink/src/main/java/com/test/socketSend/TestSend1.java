package com.test.socketSend;

import com.test.util.FileReader;
import com.test.util.URLUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;

public class TestSend1 {
	public static String contentPath = "testEvent.txt";
	public static long inv = 1000;
	public static void main(String[] args) {
		Socket client = null;
		Writer writer = null;
		BufferedReader br = null;

		String host = "localhost"; // 要连接的服务端IP地址
		int port = 9000; // 要连接的服务端对应的监听端口
		try {
			// 与服务端建立连接
			client = new Socket(host, port);
			// 建立连接后就可以往服务端写数据了
			// 客户端发送给服务端的参数，设置传递参数的编码格式为：GBK

			Long s = System.currentTimeMillis();
			writer = new OutputStreamWriter(client.getOutputStream(), "utf-8");

			Iterator<String> iterator = null;
			String path = URLUtil.baseUrl  + contentPath;
			try {
				iterator = FileReader.readFile(path).iterator();
				while (iterator.hasNext()){
					String massage = iterator.next();
					System.out.println(massage);
					writer.write(massage);
					writer.write("\n");//BufferedReader读取，记得加换行
					writer.flush();
					Thread.sleep(inv);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}


		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				if (writer != null) {
					writer.close();
				}
				if (writer != null) {
					br.close();
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
