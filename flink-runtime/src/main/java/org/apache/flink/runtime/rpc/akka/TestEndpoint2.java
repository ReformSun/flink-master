package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;

public class TestEndpoint2 extends RpcEndpoint{
	protected TestEndpoint2(RpcService rpcService, String endpointId) {
		super(rpcService, endpointId);
	}

	protected TestEndpoint2(RpcService rpcService) {
		super(rpcService);
	}

	@Override
	public CompletableFuture<Void> postStop() {
		return null;
	}
}
