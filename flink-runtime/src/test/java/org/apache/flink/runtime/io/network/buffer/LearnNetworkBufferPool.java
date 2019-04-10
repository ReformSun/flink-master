package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

public class LearnNetworkBufferPool extends TestLogger{

	@Test
	public void testMethod1() {
		final int bufferSize = 128;
		final int numBuffers = 10;
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers,bufferSize);
	}

}
