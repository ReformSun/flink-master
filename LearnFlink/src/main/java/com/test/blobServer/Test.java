package com.test.blobServer;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;

import java.io.IOException;

public class Test {
	public static void main(String[] args) {

	}


	public static void testMethod1() {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,"E:\\Asunjihua\\idea\\flink-master\\LearnFlink\\src\\main\\resources\\server");
		try {
			BlobServer blobServer = new BlobServer(config,new VoidBlobStore());


		} catch (IOException e) {
			e.printStackTrace();
		}


	}
}
