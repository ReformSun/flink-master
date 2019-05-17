package com.test.iofile.executionGraphStore;

import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;

public class TestMain1 {
	/**
	 * 3b32934545a18e722be83e995f4e37f0
	 */
	private static String jobid = "1bc8a35b2aeeadca9af3b41b10a9156a";
	public static void main(String[] args) {
//		testMethod1();
		testMethod2();
	}
	public static void testMethod1(){
		ArchivedExecutionGraph executionGraph = ReadExecutionGraphStoreFile.readExecutionGraphFile(jobid);
		JobDetails jobDetails = WebMonitorUtils.createDetailsForJob(executionGraph);

		System.out.println(jobDetails.toString());
	}

	public static void testMethod2(){
		ArchivedExecutionGraph executionGraph = ReadExecutionGraphStoreFile.readExecutionGraphFile(jobid);
		JobExceptionsInfo jobExceptionsInfo = ReadExecutionGraphStoreFile.createJobExceptionsInfo(executionGraph);
		System.out.println(jobExceptionsInfo);
	}
}
