package com.sunny.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Test {

	private static final Logger LOG = LoggerFactory.getLogger(Test.class);

	public static void main(String[] args) {
		Configuration globalConfiguration = GlobalConfiguration.loadConfiguration("E:\\Asunjihua\\idea\\flink-master\\flink-dist\\target\\flink-1.7-SNAPSHOT-bin\\flink-1.7-SNAPSHOT\\conf");
		ResourceID resourceID = ResourceID.generate();

		SignalHandler.register(LOG);


		globalConfiguration.setString("log.file","/Users/apple/Documents/AgentJava/flink-master/flink-runtime/src/main/resources/log/flink-ideaS-task.log");
		globalConfiguration.setString("log4j.configuration","file:/Users/apple/Documents/AgentJava/flink-master/flink-dist/target/flink-1.7-SNAPSHOT-bin/flink-1.7-SNAPSHOT/conf/log4j.properties");
		globalConfiguration.setString("logback.configurationFile","file:/Users/apple/Documents/AgentJava/flink-master/flink-dist/target/flink-1.7-SNAPSHOT-bin/flink-1.7-SNAPSHOT/conf/logback.xml");

		try {
			FileSystem.initialize(globalConfiguration);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			TaskManagerRunner taskManagerRunner = new TaskManagerRunner(globalConfiguration,resourceID);
			taskManagerRunner.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
