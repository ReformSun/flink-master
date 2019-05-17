package com.test.iofile.executionGraphStore;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadExecutionGraphStoreFile {
	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;
	public static final String baseU = "/Users/apple/Desktop/state/testio/executionGraphStore";
	public static ArchivedExecutionGraph readExecutionGraphFile(String jobid){
		final File archivedExecutionGraphFile = new File(baseU,jobid);
		if (archivedExecutionGraphFile.exists()) {
			try {
				try (FileInputStream fileInputStream = new FileInputStream(archivedExecutionGraphFile)) {
                    return InstantiationUtil.deserializeObject(fileInputStream, ReadExecutionGraphStoreFile.class.getClassLoader());
                }
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Could not find file for archived execution graph " + jobid +
				". This indicates that the file either has been deleted or never written.");
		}

		return null;
	}

	public static JobExceptionsInfo createJobExceptionsInfo(AccessExecutionGraph executionGraph) {
		ErrorInfo rootException = executionGraph.getFailureInfo();
		String rootExceptionMessage = null;
		Long rootTimestamp = null;
		if (rootException != null) {
			rootExceptionMessage = rootException.getExceptionAsString();
			rootTimestamp = rootException.getTimestamp();
		}

		List<JobExceptionsInfo.ExecutionExceptionInfo> taskExceptionList = new ArrayList<>();
		boolean truncated = false;
		for (AccessExecutionVertex task : executionGraph.getAllExecutionVertices()) {
			String t = task.getFailureCauseAsString();
			if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				if (taskExceptionList.size() >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				TaskManagerLocation location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
					location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";
				long timestamp = task.getStateTimestamp(ExecutionState.FAILED);
				taskExceptionList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
					t,
					task.getTaskNameWithSubtaskIndex(),
					locationString,
					timestamp == 0 ? -1 : timestamp));
			}
		}

		return new JobExceptionsInfo(rootExceptionMessage, rootTimestamp, taskExceptionList, truncated);
	}
}
