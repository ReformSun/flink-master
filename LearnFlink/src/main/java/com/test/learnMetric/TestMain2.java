package com.test.learnMetric;

import com.test.learnMetric.group.GroupUtil;
import com.test.learnMetric.group.TestMetricGroup;
import com.test.util.StreamExecutionEnvUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

/**
 * 通过metricRegistry.register(new SimpleCounter(),"aa",operatorMetricGroup); 注册测量值
 * 和调用operatorMetricGroup.counter(1);注册是一样的 都会在报道者中报道
 */
public class TestMain2 {
	private static MetricRegistryImpl metricRegistry;
	public static void main(String[] args) {
		Configuration configuration = StreamExecutionEnvUtil.getConfiguration();
		MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(configuration);
		metricRegistry = new MetricRegistryImpl(metricRegistryConfiguration);
//		testMethod1();
		testMethod2();
	}

	public static void testMethod1(){
		OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
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

	/**
	 * MeterView
	 * 每一分钟的平均值
	 */
	public static void testMethod2(){
		OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
		MeterView meterView = new MeterView(new SimpleCounter(),60);
		metricRegistry.register(meterView,"aa",operatorMetricGroup);
		for (int i = 0; i < 600; i++) {
			meterView.markEvent(10);
			meterView.update();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static OperatorMetricGroup getOperatorMetricGroup(){
		OperatorID operatorID = new OperatorID();
		TaskMetricGroup taskMetricGroup = GroupUtil.getTaskMetricGroup(metricRegistry);
		OperatorMetricGroup operatorMetricGroup = new OperatorMetricGroup(metricRegistry,taskMetricGroup,operatorID,"test");
		return operatorMetricGroup;
	}

}
