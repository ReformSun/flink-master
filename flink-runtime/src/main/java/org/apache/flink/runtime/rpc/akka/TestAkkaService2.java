package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class TestAkkaService2 {
	public static void main(String[] args) {
		Properties propertie = new Properties();
		propertie.setProperty("akka.actor.provider","akka.remote.RemoteActorRefProvider");
//		propertie.setProperty("akka.remote.netty.tcp.hostname", "127.0.0.1");
//		propertie.setProperty("akka.remote.netty.tcp.port", "8082");

		Configuration configuration = ConfigurationUtils.createConfiguration(propertie);

		try {
			RpcService rpcService = AkkaRpcServiceUtils.createRpcService("127.0.0.1",8081,configuration);

//			TestEndpoint2 testRpcEndpoint = new TestEndpoint2(rpcService,"TestEndpoint2");

			CompletableFuture<RpcGateway> testEndpoint2 = rpcService.connect("akka.tcp://flink@127.0.0.1:8082/user/TestRpcEndpoint",RpcGateway.class);
			RpcGateway testEndpoint21 = testEndpoint2.get();

			System.out.println(testEndpoint21.getAddress());

			rpcService.stopService();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
