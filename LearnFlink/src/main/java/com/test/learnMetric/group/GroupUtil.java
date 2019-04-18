package com.test.learnMetric.group;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

public class GroupUtil {
	public static TaskMetricGroup getTaskMetricGroup(MetricRegistry metricRegistry){
		JobVertexID vertexId  = new JobVertexID();
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
		String taskName = "taskName";
		int subtaskIndex = 1;
		int attemptNumber = 1;

		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(metricRegistry,
			getTaskManagerJobMetricGroup(metricRegistry),vertexId,
			executionAttemptID,taskName,subtaskIndex,attemptNumber);

		return taskMetricGroup;
	}

	public static TaskManagerJobMetricGroup getTaskManagerJobMetricGroup(MetricRegistry metricRegistry){
		JobID jobID = new JobID();
		String jobName = "jobName";
		TaskManagerJobMetricGroup taskManagerJobMetricGroup = new TaskManagerJobMetricGroup(metricRegistry
			,getTaskManagerMetricGroup(metricRegistry),jobID,jobName);
		return taskManagerJobMetricGroup;
	}

	public static TaskManagerMetricGroup getTaskManagerMetricGroup(MetricRegistry metricRegistry){
		String hostname = "localhost";
		String taskManagerId = "taskManagerId";
		TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(metricRegistry,hostname,taskManagerId);
		return taskManagerMetricGroup;
	}



}
