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
 * Interface for operators that can perform snapshots of their state.
 *
 * @param <S> Generic type of the state object that is created as handle to snapshots.
 * @param <R> Generic type of the state object that used in restore.
 */
public interface Snapshotable<S extends StateObject, R> {

	/**
	 * Operation n. 操作；经营；[外科] 手术；[数][计] 运算
	 * Options n. 选择；期权；[计] 选择项（option的复数） v. 给予…的销售权；为…提供选择供应的附件（option的三单形式）
	 * perform vt. 执行；完成；演奏 vi. 执行，机器运转；表演
	 * yield vt. 屈服；出产，产生；放弃 vi. 屈服，投降 n. 产量；收益
	 * runnable adj. 可追捕的；可猎取的；适合猎取的
	 * future n. 未来；前途；期货；将来时 adj. 将来的，未来的
	 * In the later case 在后来的情况下
	 * handle n. [建] 把手；柄；手感；口实 vt. 处理；操作；运用；买卖；触摸 vi. 搬运；易于操纵
	 */
	/**
	 * 写一个快照到流中 给予了一个CheckpointStreamFactory和RunnableFuture 给一个状态这个处理快照
	 * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory} and
	 * 执行同步或者异步有实现类决定
	 * returns a @{@link RunnableFuture} that gives a state handle to the snapshot. It is up to the implementation if
	 * 在后来的情况下 返回的Runnable必须首先执行在得到句柄之前
	 * the operation is performed synchronous or asynchronous. In the later case, the returned Runnable must be executed
	 * first before obtaining the handle.
	 *                  	检查点id
	 * @param checkpointId  The ID of the checkpoint.
	 *                      检查点时间戳
	 * @param timestamp     The timestamp of the checkpoint.
	 *                      这个工厂类能够写状态到流中
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 *						如何执行此检查点的选项。
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return A runnable future that will yield a {@link StateObject}.
	 */
	RunnableFuture<S> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) throws Exception;

	/**
	 * Restores vt. 恢复；修复；归还 vi. 恢复；还原
	 * previously adv. 以前；预先；仓促地
	 * snapshot n. 快照，快相；急射，速射；简单印象 vt. 给…拍快照 vi. 拍快照
	 * Typically adv. 代表性地；作为特色地
	 * handles n. [建] 把手；柄；手感；口实 vt. 处理；操作；运用；买卖；触摸 vi. 搬运；易于操纵
	 */
	/**
	 * 从提供的参数的以前的快照中恢复状态 参数是读取老的状态的状态句柄
	 * Restores state that was previously snapshotted from the provided parameters. Typically the parameters are state
	 * handles from which the old state is read.
	 *
	 * @param state the old state to restore.
	 */
	void restore(R state) throws Exception;
}
