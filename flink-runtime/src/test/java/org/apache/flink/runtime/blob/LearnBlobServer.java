package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.io.IOException;

public class LearnBlobServer {

	@Test
	public void testMethod1() {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,"E:\\Asunjihua\\idea\\flink-master\\LearnFlink\\src\\main\\resources\\server\\");
		try {
			BlobServer blobServer = new BlobServer(config,new VoidBlobStore());
			blobServer.start();


			byte[] bytes = "aaaaaa".getBytes();

			BlobKey by = blobServer.putPermanent(new JobID(),bytes);
			System.out.println(by.getType());


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
