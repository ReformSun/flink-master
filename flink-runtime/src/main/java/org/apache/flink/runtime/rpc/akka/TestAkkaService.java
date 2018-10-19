package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Properties;

public class TestAkkaService {
	public static void main(String[] args) {
		Properties propertie = new Properties();
		propertie.setProperty("akka.actor.provider","akka.remote.RemoteActorRefProvider");
//		propertie.setProperty("akka.remote.netty.tcp.hostname", "127.0.0.1");
//		propertie.setProperty("akka.remote.netty.tcp.port", "8082");

		Configuration configuration = ConfigurationUtils.createConfiguration(propertie);

		try {
			RpcService rpcService = AkkaRpcServiceUtils.createRpcService("127.0.0.1",8082,configuration);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
