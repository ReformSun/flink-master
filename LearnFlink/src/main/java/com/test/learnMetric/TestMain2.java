package com.test.learnMetric;

import com.test.learnMetric.group.GroupUtil;
import com.test.learnMetric.group.TestMetricGroup;
import com.test.util.StreamExecutionEnvUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

public class TestMain2 {
	private static MetricRegistryImpl metricRegistry;
	public static void main(String[] args) {
		Configuration configuration = StreamExecutionEnvUtil.getConfiguration();
		MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(configuration);
		metricRegistry = new MetricRegistryImpl(metricRegistryConfiguration);
		testMethod1();
	}

	public static void testMethod1(){
		OperatorID operatorID = new OperatorID();
		TaskMetricGroup taskMetricGroup = GroupUtil.getTaskMetricGroup(metricRegistry);
		OperatorMetricGroup operatorMetricGroup = new OperatorMetricGroup(metricRegistry,taskMetricGroup,operatorID,"test");
		metricRegistry.register(new SimpleCounter(),"aa",operatorMetricGroup);
		Counter counter = operatorMetricGroup.counter(1);
		for (int i = 0; i < 100; i++) {
			counter.inc();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}



}
