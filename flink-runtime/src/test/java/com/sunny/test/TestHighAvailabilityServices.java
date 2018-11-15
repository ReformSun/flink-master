package com.sunny.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;

import java.util.UUID;
import java.util.concurrent.ExecutorService;


final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {

	}

	@Override
	public void handleError(Exception exception) {

	}
}

public class TestHighAvailabilityServices {
	public static void main(String[] args) {
		ExecutorService executor = java.util.concurrent.Executors.newScheduledThreadPool(
			Hardware.getNumberCPUCores(),
			new ExecutorThreadFactory("taskmanager-future"));
		Configuration globalConfiguration = GlobalConfiguration.loadConfiguration("/Users/apple/Documents/AgentJava/flink-master/flink-dist/target/flink-1.7-SNAPSHOT-bin/flink-1.7-SNAPSHOT/conf");

		try {
			HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
                globalConfiguration,
                executor,
                HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

			LeaderRetrievalService resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());


		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
