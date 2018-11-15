package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class TestResourceManagerGatewayImp extends RpcEndpoint implements ResourceManagerGateway{

	protected TestResourceManagerGatewayImp(RpcService rpcService, String endpointId) {
		super(rpcService, endpointId);
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(JobMasterId jobMasterId, ResourceID jobMasterResourceId, String jobMasterAddress, JobID jobId, Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(JobMasterId jobMasterId, SlotRequest slotRequest, Time timeout) {
		return null;
	}

	@Override
	public void cancelSlotRequest(AllocationID allocationID) {

	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(String taskExecutorAddress, ResourceID resourceId, int dataPort, HardwareDescription hardwareDescription, Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
		return null;
	}

	@Override
	public void notifySlotAvailable(InstanceID instanceId, SlotID slotID, AllocationID oldAllocationId) {

	}

	@Override
	public void registerInfoMessageListener(String infoMessageListenerAddress) {

	}

	@Override
	public void unRegisterInfoMessageListener(String infoMessageListenerAddress) {

	}

	@Override
	public CompletableFuture<Acknowledge> deregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
		return null;
	}

	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return null;
	}

	@Override
	public void heartbeatFromTaskManager(ResourceID heartbeatOrigin, SlotReport slotReport) {

	}

	@Override
	public void heartbeatFromJobManager(ResourceID heartbeatOrigin) {

	}

	@Override
	public void disconnectTaskManager(ResourceID resourceID, Exception cause) {

	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {

	}

	@Override
	public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID taskManagerId, Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerFileUpload(ResourceID taskManagerId, FileType fileType, Time timeout) {
		return null;
	}

	@Override
	public ResourceManagerId getFencingToken() {
		return null;
	}

	@Override
	public CompletableFuture<Void> postStop() {
		return null;
	}

	@Override
	public String getAddress() {
		return null;
	}

	@Override
	public String getHostname() {
		return null;
	}
}
