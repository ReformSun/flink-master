package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;

public class TestRpcEndpoint extends RpcEndpoint implements TestGateway{

	protected TestRpcEndpoint(RpcService rpcService, String endpointId) {
		super(rpcService, endpointId);
	}

	protected TestRpcEndpoint(RpcService rpcService) {
		super(rpcService);
	}

	@Override
	public CompletableFuture<Void> postStop() {
		return null;
	}

	@Override
	public void testMethod() {
		System.out.println("dd");
	}
}
