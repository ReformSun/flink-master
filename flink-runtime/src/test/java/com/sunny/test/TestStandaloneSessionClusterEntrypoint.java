package com.sunny.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class TestStandaloneSessionClusterEntrypoint extends StandaloneSessionClusterEntrypoint{
	public TestStandaloneSessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	public static void main(String[] args) {
		Configuration globalConfiguration = GlobalConfiguration.loadConfiguration("/Users/apple/Documents/AgentJava/flink-master/flink-dist/target/flink-1.7-SNAPSHOT-bin/flink-1.7-SNAPSHOT/conf");

		TestStandaloneSessionClusterEntrypoint entrypoint = new TestStandaloneSessionClusterEntrypoint(globalConfiguration);

		entrypoint.startCluster();

	}

}
