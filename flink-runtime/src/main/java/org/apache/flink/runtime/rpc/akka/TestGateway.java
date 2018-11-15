package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.rpc.RpcGateway;

public interface TestGateway extends RpcGateway{

	public void testMethod();

}
