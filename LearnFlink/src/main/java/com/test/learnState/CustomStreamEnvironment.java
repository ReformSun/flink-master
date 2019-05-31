package com.test.learnState;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class CustomStreamEnvironment extends StreamExecutionEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

	private final Configuration configuration;
	private SavepointRestoreSettings savepointRestoreSettings;

	public void setSavepointRestoreSettings(final SavepointRestoreSettings savepointRestoreSettings) {
		this.savepointRestoreSettings = savepointRestoreSettings;
	}

	public CustomStreamEnvironment() {
		this(new Configuration());
	}

	public CustomStreamEnvironment(@Nonnull Configuration configuration) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
				"The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
					"or running in a TestEnvironment context.");
		}
		this.configuration = configuration;
		setParallelism(1);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		// transform the streaming program into a JobGraph
		StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);

		JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.setAllowQueuedScheduling(true);
		if (savepointRestoreSettings != null){
			jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
		}

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());
		configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");

		// add (and override) the settings with what the user defined
		configuration.addAll(this.configuration);

		if (!configuration.contains(RestOptions.PORT)) {
			configuration.setInteger(RestOptions.PORT, 0);
		}

		int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
			.build();

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		MiniCluster miniCluster = new MiniCluster(cfg);

		try {
			miniCluster.start();
			configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());
			String address = miniCluster.getRestAddress().getAuthority();

			return miniCluster.executeJobBlocking(jobGraph);
		}
		finally {
			transformations.clear();
			miniCluster.close();
		}
	}
}
