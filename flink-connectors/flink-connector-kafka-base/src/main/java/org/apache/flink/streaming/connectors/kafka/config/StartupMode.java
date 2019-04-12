/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;

/**
 * Startup modes for the Kafka Consumer.
 */
@Internal
public enum StartupMode {

	/** Start from committed offsets in ZK / Kafka brokers of a specific consumer group (default). */
	GROUP_OFFSETS(KafkaTopicPartitionStateSentinel.GROUP_OFFSET),

	/** Start from the earliest offset possible. 从最早可能的偏移量开始*/
	EARLIEST(KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET),

	/** Start from the latest offset. 从最新的偏移量开始*/
	LATEST(KafkaTopicPartitionStateSentinel.LATEST_OFFSET),

	/**
	 * Start from user-supplied timestamp for each partition. 开始从用户提供的每个分区的时间戳开始
	 * Since this mode will have specific offsets to start with, we do not need a sentinel value;
	 * using Long.MIN_VALUE as a placeholder. 使用Long最小值作为占位符
	 */
	TIMESTAMP(Long.MIN_VALUE),

	/**
	 * Start from user-supplied specific offsets for each partition. 开始从用户指定的每一个分区的特殊偏移量开始
	 * Since this mode will have specific offsets to start with, we do not need a sentinel value;
	 * using Long.MIN_VALUE as a placeholder.
	 */
	SPECIFIC_OFFSETS(Long.MIN_VALUE);

	/** The sentinel offset value corresponding to this startup mode. */
	private long stateSentinel;

	StartupMode(long stateSentinel) {
		this.stateSentinel = stateSentinel;
	}

	public long getStateSentinel() {
		return stateSentinel;
	}
}
