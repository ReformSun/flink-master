/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 当个任务生成的数据的结果分区
 * A result partition for data produced by a single task.
 *
 * <p> 这个类是一个逻辑中间结果分区运行时的部分，实质上，一个结果分区是一个缓冲区实例的集合
 * This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * 这些缓冲区是有一个或者多个子结果分区实例组成，子结果分区根据消费者任务和数据的数量进一步分区
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances, which further partition the data depending on the
 * number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p> Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *每个结果分区都有三个阶段的生命周期 生产，消费，释放
 * The life-cycle of each result partition has three (possibly overlapping) phases:
 * <ol>
 * <li><strong>Produce</strong>: </li>
 * <li><strong>Consume</strong>: </li>
 * <li><strong>Release</strong>: </li>
 * </ol>
 *
 * <h2>Lazy deployment and updates of consuming tasks</h2>
 *
 * Before a consuming task can request the result, it has to be deployed. The time of deployment
 * depends on the PIPELINED vs. BLOCKING characteristic of the result partition. With pipelined
 * results, receivers are deployed as soon as the first buffer is added to the result partition.
 * With blocking results on the other hand, receivers are deployed after the partition is finished.
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
public class ResultPartition implements ResultPartitionWriter, BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);
	
	private final String owningTaskName;

	private final TaskActions taskActions;

	private final JobID jobId;

	private final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	private final ResultPartitionType partitionType;

	/** The subpartitions of this partition. At least one. 这个分区的所有自分区，最少一个*/
	private final ResultSubpartition[] subpartitions;

	private final ResultPartitionManager partitionManager;

	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;

	public final int numTargetKeyGroups;

	private final boolean sendScheduleOrUpdateConsumersMessage;

	// - Runtime state --------------------------------------------------------
	/**
	 * 标注结果分区的状态，是否被释放，为true已被释放
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/**
	 * 被引用的结果子分区数（一个结果分区有多个自分区，每个自分区对应多个下级的单个子任务。每个子任务会消费
	 * 对应的自分区数据，当自分区对应的消费者都没有消费完时。原子计数器总数为自分区数。每消费完一个减去1）
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private final AtomicInteger pendingReferences = new AtomicInteger();

	private BufferPool bufferPool;

	private boolean hasNotifiedPipelinedConsumers;

	private boolean isFinished;

	private volatile Throwable cause;

	public ResultPartition(
		String owningTaskName,
		TaskActions taskActions, // actions on the owning task
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		IOManager ioManager,
		boolean sendScheduleOrUpdateConsumersMessage) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.taskActions = checkNotNull(taskActions);
		this.jobId = checkNotNull(jobId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.subpartitions = new ResultSubpartition[numberOfSubpartitions];
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionManager = checkNotNull(partitionManager);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;

		// Create the subpartitions.
		switch (partitionType) {
			case BLOCKING:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new SpillableSubpartition(i, this, ioManager);
				}

				break;

			case PIPELINED:
			case PIPELINED_BOUNDED:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new PipelinedSubpartition(i, this);
				}

				break;

			default:
				throw new IllegalArgumentException("Unsupported result partition type.");
		}

		// Initially, partitions should be consumed once before release.
		pin();

		LOG.debug("{}: Initialized {}", owningTaskName, this);
	}

	/**
	 * Registers a buffer pool with this result partition.
	 * <p>
	 * There is one pool for each result partition, which is shared by all its sub partitions.
	 * <p>
	 * The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskManager}.
	 */
	public void registerBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
				"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		this.bufferPool = checkNotNull(bufferPool);

		// If the partition type is back pressure-free, we register with the buffer pool for
		// callbacks to release memory.
		if (!partitionType.hasBackPressure()) {
			bufferPool.setBufferPoolOwner(this);
		}
	}

	public JobID getJobId() {
		return jobId;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return subpartitions.length;
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	/**
	 * Returns the type of this result partition.
	 *
	 * @return result partition type
	 */
	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	// ------------------------------------------------------------------------

	@Override
	public void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		checkNotNull(bufferConsumer);

		ResultSubpartition subpartition;
		try {
			checkInProduceState();
			subpartition = subpartitions[subpartitionIndex];
		}
		catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		if (subpartition.add(bufferConsumer)) {
			notifyPipelinedConsumers();
		}
	}

	@Override
	public void flushAll() {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		subpartitions[subpartitionIndex].flush();
	}

	/**
	 * Finishes the result partition.
	 *
	 * <p> After this operation, it is not possible to add further data to the result partition.
	 *
	 * <p> For BLOCKING results, this will trigger the deployment of consuming tasks.
	 */
	public void finish() throws IOException {
		boolean success = false;

		try {
			checkInProduceState();

			for (ResultSubpartition subpartition : subpartitions) {
				subpartition.finish();
			}

			success = true;
		}
		finally {
			if (success) {
				isFinished = true;

				notifyPipelinedConsumers();
			}
		}
	}

	public void release() {
		release(null);
	}

	/**
	 * 释放 的意思不是把类释放掉 而是把管理的生产者生产的数据释放掉
	 * Releases the result partition.
	 */
	public void release(Throwable cause) {
		if (isReleased.compareAndSet(false, true)) {
			LOG.debug("{}: Releasing {}.", owningTaskName, this);

			// Set the error cause
			if (cause != null) {
				this.cause = cause;
			}

			// Release all subpartitions
			for (ResultSubpartition subpartition : subpartitions) {
				try {
					subpartition.release();
				}
				// Catch this in order to ensure that release is called on all subpartitions
				catch (Throwable t) {
					LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
				}
			}
		}
	}

	public void destroyBufferPool() {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
	}

	/**
	 * Returns the requested subpartition.
	 */
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		int refCnt = pendingReferences.get();

		checkState(refCnt != -1, "Partition released.");
		checkState(refCnt > 0, "Partition not pinned.");

		checkElementIndex(index, subpartitions.length, "Subpartition not found.");

		ResultSubpartitionView readView = subpartitions[index].createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
	}

	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	/**
	 * Releases buffers held by this result partition.
	 *
	 * <p> This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition subpartition : subpartitions) {
			toRelease -= subpartition.releaseMemory();

			// Only release as much memory as needed
			if (toRelease <= 0) {
				break;
			}
		}
	}

	@Override
	public String toString() {
		return "ResultPartition " + partitionId.toString() + " [" + partitionType + ", "
				+ subpartitions.length + " subpartitions, "
				+ pendingReferences + " pending references]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Pins the result partition.
	 *
	 * <p> The partition can only be released after each subpartition has been consumed once per pin
	 * operation.
	 */
	void pin() {
		while (true) {
			int refCnt = pendingReferences.get();

			if (refCnt >= 0) {
				if (pendingReferences.compareAndSet(refCnt, refCnt + subpartitions.length)) {
					break;
				}
			}
			else {
				throw new IllegalStateException("Released.");
			}
		}
	}

	/**
	 * 通知一个子分区已经被释放掉
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {

		// 结果分区已被释放 则直接返回
		if (isReleased.get()) {
			return;
		}
		// 原子计数器减1后 获得未完成的子分区计数
		int refCnt = pendingReferences.decrementAndGet();
		// 如果全部完成，通知分区管理器，他会把结果分区释放掉
		if (refCnt == 0) {
			partitionManager.onConsumedPartition(this);
		}
		else if (refCnt < 0) {
			throw new IllegalStateException("All references released.");
		}

		LOG.debug("{}: Received release notification for subpartition {} (reference count now at: {}).",
				this, subpartitionIndex, pendingReferences);
	}

	ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}

	// ------------------------------------------------------------------------

	private void checkInProduceState() throws IllegalStateException {
		checkState(!isFinished, "Partition already finished.");
	}

	/**
	 * Notifies pipelined consumers of this result partition once.
	 */
	private void notifyPipelinedConsumers() {
		if (sendScheduleOrUpdateConsumersMessage && !hasNotifiedPipelinedConsumers && partitionType.isPipelined()) {
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionId, taskActions);

			hasNotifiedPipelinedConsumers = true;
		}
	}
}
