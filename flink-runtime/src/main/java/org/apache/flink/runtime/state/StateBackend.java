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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;

/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored and
 * checkpointed. Different State Backends store their state in different fashions, and use
 * different data structures to hold the state of a running application.
 *
 * <p>For example, the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend memory state backend}
 * keeps working state in the memory of the TaskManager and stores checkpoints in the memory of the
 * JobManager. The backend is lightweight and without additional dependencies, but not highly available
 * and supports only small state.
 *
 * <p>The {@link org.apache.flink.runtime.state.filesystem.FsStateBackend file system state backend}
 * keeps working state in the memory of the TaskManager and stores state checkpoints in a filesystem
 * (typically a replicated highly-available filesystem, like <a href="https://hadoop.apache.org/">HDFS</a>,
 * <a href="https://ceph.com/">Ceph</a>, <a href="https://aws.amazon.com/documentation/s3/">S3</a>,
 * <a href="https://cloud.google.com/storage/">GCS</a>, etc).
 * 
 * <p>The {@code RocksDBStateBackend} stores working state in <a href="http://rocksdb.org/">RocksDB</a>,
 * and checkpoints the state by default to a filesystem (similar to the {@code FsStateBackend}).
 * 
 * <h2>Raw Bytes Storage and Backends</h2>
 * 
 * The {@code StateBackend} creates services for <i>raw bytes storage</i> and for <i>keyed state</i>
 * and <i>operator state</i>.
 * 
 * <p>The <i>raw bytes storage</i> (through the {@link CheckpointStreamFactory}) is the fundamental
 * service that simply stores bytes in a fault tolerant fashion. This service is used by the JobManager
 * to store checkpoint and recovery metadata and is typically also used by the keyed- and operator state
 * backends to store checkpointed state.
 *
 * <p>The {@link AbstractKeyedStateBackend} and {@link OperatorStateBackend} created by this state
 * backend define how to hold the working state for keys and operators. They also define how to checkpoint
 * that state, frequently using the raw bytes storage (via the {@code CheckpointStreamFactory}).
 * However, it is also possible that for example a keyed state backend simply implements the bridge to
 * a key/value store, and that it does not need to store anything in the raw byte storage upon a
 * checkpoint.
 * 
 * <h2>Serializability</h2>
 * 
 * State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application code. 
 * 
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses
 * of {@link AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper
 * states stores that provide access to the persistent storage and hold the keyed- and operator
 * state data structures. That way, the State Backend can be very lightweight (contain only
 * configurations) which makes it easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 * 
 * State backend implementations have to be thread-safe. Multiple threads may be creating
 * streams and keyed-/operator state backends concurrently.
 */
@PublicEvolving
public interface StateBackend extends java.io.Serializable {

	/**
	 *
	 * durable adj 耐用的 持久的 n 耐用品
	 * storage n 存储 仓库 储藏所
	 * persistence n. 持续；固执；存留；坚持不懈；毅力
	 */
	// ------------------------------------------------------------------------
	//  Checkpoint storage - the durable persistence of checkpoint data
//	    检查点储藏所   检查点数据持续持久化存储
	// ------------------------------------------------------------------------

	/**
	 * Resolves vt. 决定；溶解；使…分解；决心要做… vi. 解决；决心；分解 n. 坚决；决定要做的事
	 * pointer n. 指针；指示器；教鞭；暗示
	 * given adj. 赠予的；沉溺的；规定的 prep. 考虑到 v. 给予（give的过去分词）
	 * supports n. 支持；支承结构；忍受；[法] 赡养（support的复数） v. 支持，忍受（support单三形式）
	 * metadata n. [计] 元数据
	 * dispose vt. 处理；处置；安排 vi. 处理；安排；（能够）决定 n. 处置；性情
	 * backend n 后端
	 * understand vt. 理解；懂；获悉；推断；省略；明白 vi. 理解；懂得；熟悉
	 * format n. 格式；版式；开本 vt. 使格式化；规定…的格式 vi. 设计版式
	 * external adj. 外部的；表面的；[药] 外用的；外国的；外面的 n. 外部；外观；外面
	 * handle n. [建] 把手；柄；手感；口实 vt. 处理；操作；运用；买卖；触摸 vi. 搬运；易于操纵
	 */
	/**
	 * 解析规定的指向检查点或者保存点位置的指针
	 * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
	 * 此位置支持读取检查点元数据 或者 处理检查点所储藏位置
	 * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
	 * 如果状态后端不能够获悉指针的格式 （例如指针被不同的状态后端创建）这个方法会抛出io异常
	 * <p>If the state backend cannot understand the format of the pointer (for example because it
	 * was created by a different state backend) this method should throw an {@code IOException}.
	 * 被分解外部检查点指针
	 * @param externalPointer The external checkpoint pointer to resolve.
	 * 处理后的检查点位置
	 * @return The checkpoint location handle.
	 *
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

	/**
	 * Creates a storage for checkpoints for the given job. The checkpoint storage is
	 * used to write checkpoint data and metadata.
	 *
	 * @param jobId The job to store checkpoint data for.
	 * @return A checkpoint storage for the given job.
	 *
	 * @throws IOException Thrown if the checkpoint storage cannot be initialized.
	 */
	CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException;

	// ------------------------------------------------------------------------
	//  Structure Backends 
	// ------------------------------------------------------------------------

	/**
	 * responsible adj. 负责的，可靠的；有责任的
	 * bound adj. 有义务的；必定的；受约束的；装有封面的 vt. 束缚（bind的过去式，过去分词）；使跳跃 n. 范围；跳跃 vi. 限制；弹起
	 * organized adj. 有组织的；安排有秩序的；做事有条理的 v. 组织（organize的过去分词）
	 * range n. 范围；幅度；排；山脉 vi. （在...内）变动；平行，列为一行；延伸；漫游；射程达到 vt. 漫游；放牧；使并列；归类于；来回走动
	 * group n. 组；团体 adj. 群的；团体的 vi. 聚合 vt. 把…聚集；把…分组
	 */
	/**
	 * 创建一个新的可靠存储key状态和检查点的状态后台
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it. Uses default TTL time provider.
	 *
	 * <p>Keyed State 是绑定了一个key的状态
	 * <i>Keyed State</i> is state where each value is bound to a key.
	 * 把状态组织起来的key的类型
	 * @param <K> The type of the keys by which the state is organized.
	 * 把key状态后端给予的job，算子，和key 通过他们进行分组
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	default <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception {
		return createKeyedStateBackend(
			env,
			jobID,
			operatorIdentifier,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			kvStateRegistry,
			TtlTimeProvider.DEFAULT);
	}

	/**
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it.
	 *
	 * <p><i>Keyed State</i> is state where each value is bound to a key.
	 *
	 * @param <K> The type of the keys by which the state is organized.
	 *
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	<K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider) throws Exception;
	
	/**
	 * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
	 *
	 * <p>Operator state is state that is associated with parallel operator (or function) instances,
	 * rather than with keys.
	 *
	 * @param env The runtime environment of the executing task.
	 * @param operatorIdentifier The identifier of the operator whose state should be stored.
	 *
	 * @return The OperatorStateBackend for operator identified by the job and operator identifier.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception;
}
