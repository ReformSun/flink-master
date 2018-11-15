package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.rpc.akka.TestRpcEndpoint;

import java.util.Properties;

public class TestResourceManager {

	public static void main(String[] args) {
		Properties propertie = new Properties();
		propertie.setProperty("akka.actor.provider","akka.remote.RemoteActorRefProvider");
//		propertie.setProperty("akka.remote.netty.tcp.hostname", "127.0.0.1");
//		propertie.setProperty("akka.remote.netty.tcp.port", "8082");

		Configuration configuration = ConfigurationUtils.createConfiguration(propertie);

		try {
			RpcService rpcService = AkkaRpcServiceUtils.createRpcService("localhost",6123,configuration);
			// akka.tcp://flink@localhost:6123/user/resourcemanager(00000000000000000000000000000000)
			testMethod2(rpcService);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(RpcService rpcService) throws Exception {
		TestResourceManagerGatewayImp testRpcEndpoint = new TestResourceManagerGatewayImp(rpcService,"resourcemanager");
		testRpcEndpoint.start();
	}


	public static void testMethod2(RpcService rpcService) throws Exception {
		TestFenceResourceManagerGatewayImp testRpcEndpoint = new TestFenceResourceManagerGatewayImp(rpcService,"resourcemanager");

		testRpcEndpoint.start();
	}
}
