/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import java.util.concurrent.RunnableFuture;

/**
 * approach ə'protʃ n. 方法；途径；接近 vt. 接近；着手处理 vi. 靠近
 * for prep. 为，为了；因为；给；对于；至于；适合于 conj. 因为
 * snapshot n. 快照，快相；急射，速射；简单印象vt. 给…拍快照 vi. 拍快照
 * ideally adv. 理想地；观念上地
 * stateless adj. 没有国家的；无国家主权的
 * thread safe 线程安全
 * or conj. 或，或者；还是；要不然
 * at least 至少
 *
 *
 */

/**
 *
 * Interface for different snapshot approaches in state backends. Implementing classes should ideally be stateless or at
 *
 * least threadsafe, i.e. this is a functional interface and is can be called in parallel by multiple checkpoints.
 *
 * @param <S> type of the returned state object that represents the result of the snapshot operation.
 */
@FunctionalInterface
public interface SnapshotStrategy<S extends StateObject> {


	/**
	 * Operation n. 操作；经营；[外科] 手术；[数][计] 运算
	 * future n. 未来；前途；期货；将来时 adj. 将来的，未来的
	 * Runnable adj. 可追捕的；可猎取的；适合猎取的
	 *
	 * 把快照写进CheckpointStreamFactory给的流的操作并且返回一个有状态句柄RunnableFuture给快照
	 * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory} and
	 * returns a @{@link RunnableFuture} that gives a state handle to the snapshot. It is up to the implementation if
	 * the operation is performed synchronous or asynchronous. In the later case, the returned Runnable must be executed
	 * first before obtaining the handle.
	 *
	 * @param checkpointId      The ID of the checkpoint.
	 * @param timestamp         The timestamp of the checkpoint.
	 * @param streamFactory     The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return A runnable future that will yield a {@link StateObject}.
	 */
	RunnableFuture<S> performSnapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception;
}
