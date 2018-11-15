package org.apache.flink.runtime.rpc.akka;

public class TestGatewayImp implements TestGateway{
	@Override
	public void testMethod() {

	}

	@Override
	public String getAddress() {
		return "dd";
	}

	@Override
	public String getHostname() {
		return "ccc";
	}
}
