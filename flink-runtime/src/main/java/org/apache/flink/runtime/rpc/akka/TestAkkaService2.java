package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TestAkkaService2 {
	public static void main(String[] args) {
		Properties propertie = new Properties();
		propertie.setProperty("akka.actor.provider","akka.remote.RemoteActorRefProvider");
//		propertie.setProperty("akka.remote.netty.tcp.hostname", "127.0.0.1");
//		propertie.setProperty("akka.remote.netty.tcp.port", "8082");

		Configuration configuration = ConfigurationUtils.createConfiguration(propertie);

		try {
			RpcService rpcService = AkkaRpcServiceUtils.createRpcService("127.0.0.1",8081,configuration);
			testMethod1(rpcService);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 测试recourceManager
	 * @param rpcService
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void testMethod1(RpcService rpcService) throws ExecutionException, InterruptedException {
		CompletableFuture<ResourceManagerGateway> testEndpoint2 = rpcService.connect("akka.tcp://flink@127.0.0.1:6123/user/resourcemanager",ResourceManagerGateway.class);
		ResourceManagerGateway resourceManagerGateway = testEndpoint2.get();

//		System.out.println(testEndpoint21.getAddress());
		resourceManagerGateway.registerTaskExecutor("", ResourceID.generate(),8080,null, Time.milliseconds(10));

		rpcService.stopService();

	}

	public static void testMethod2(RpcService rpcService){

	}

	public static void testMethod3(RpcService rpcService){

	}
}
