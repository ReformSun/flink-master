package org.apache.flink.runtime.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link MetricRegistryImpl}.
 */
public class LearnMetric {
	MetricRegistryImpl metricRegistry;
	@Before
	public void before(){
		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
	}

	@Test
	public void testMethod1(){
		JobID jid = new JobID();
		TaskManagerMetricGroup tm = new TaskManagerMetricGroup(metricRegistry, "host", "id");
		TaskManagerJobMetricGroup job = new TaskManagerJobMetricGroup(metricRegistry, tm, jid, "jobname");

//		QueryScopeInfo.JobQueryScopeInfo info = job.createQueryServiceMetricInfo(new DummyCharacterFilter());
//		assertEquals("", info.scope);
//		assertEquals(jid.toString(), info.jobID);
	}
}
