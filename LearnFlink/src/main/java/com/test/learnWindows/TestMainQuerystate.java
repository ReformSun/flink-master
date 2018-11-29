package com.test.learnWindows;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.net.UnknownHostException;

public class TestMainQuerystate {
	public static void main(String[] args) {
		try {
			QueryableStateClient client = new QueryableStateClient("10.4.247.17", 9069);
			client.setExecutionConfig(new ExecutionConfig());


		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
