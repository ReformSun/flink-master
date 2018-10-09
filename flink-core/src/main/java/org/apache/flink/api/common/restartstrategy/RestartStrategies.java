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

package org.apache.flink.api.common.restartstrategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * generate 动词 使形成 生殖 发生
 *
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * <p>The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
 */
@PublicEvolving
public class RestartStrategies {

	/**
	 * 形成不重新开始策略配置
	 * Generates NoRestartStrategyConfiguration.
	 *
	 * @return NoRestartStrategyConfiguration
	 */
	public static RestartStrategyConfiguration noRestart() {
		return new NoRestartStrategyConfiguration();
	}

	public static RestartStrategyConfiguration fallBackRestart() {
		return new FallbackRestartStrategyConfiguration();
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayBetweenAttempts Delay in-between restart attempts for the FixedDelayRestartStrategy 默认单位为毫秒
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, long delayBetweenAttempts) {
		return fixedDelayRestart(restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, Time delayInterval) {
		return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
	}

	/**
	 * failure 失败 故障 失败者 破产
	 * rate 比率 率
	 * Generates a FailureRateRestartStrategyConfiguration.
	 *
	 *
	 * @param failureRate Maximum number of restarts in given interval {@code failureInterval} before failing a job 一个任务失败之前 在给定的时间间隔內最大的重新启动次数
	 * @param failureInterval Time interval for failures 失败的时间间隔
	 * @param delayInterval Delay in-between restart attempts 两次尝试重新开始的延迟时间
	 */
	public static FailureRateRestartStrategyConfiguration failureRateRestart(
			int failureRate, Time failureInterval, Time delayInterval) {
		return new FailureRateRestartStrategyConfiguration(failureRate, failureInterval, delayInterval);
	}

	/**
	 * Abstract configuration for restart strategies.
	 */
	public abstract static class RestartStrategyConfiguration implements Serializable {
		private static final long serialVersionUID = 6285853591578313960L;

		private RestartStrategyConfiguration() {}

		/**
		 * Returns a description which is shown in the web interface.
		 *
		 * @return Description of the restart strategy
		 */
		public abstract String getDescription();
	}

	/**
	 * Configuration representing no restart strategy.
	 */
	public static final class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -5894362702943349962L;

		@Override
		public String getDescription() {
			return "Restart deactivated.";
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			return o instanceof NoRestartStrategyConfiguration;
		}

		@Override
		public int hashCode() {
			return Objects.hash();
		}
	}

	/**
	 * fixed 固定的 确定的 稳定的
	 * Configuration representing a fixed delay restart strategy.
	 */
	public static final class FixedDelayRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 4149870149673363190L;

		// 尝试重新开始的次数
		private final int restartAttempts;
		// 每次尝试重新开始延迟时间
		private final Time delayBetweenAttemptsInterval;

		FixedDelayRestartStrategyConfiguration(int restartAttempts, Time delayBetweenAttemptsInterval) {
			this.restartAttempts = restartAttempts;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getRestartAttempts() {
			return restartAttempts;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public int hashCode() {
			int result = restartAttempts;
			result = 31 * result + (delayBetweenAttemptsInterval != null ? delayBetweenAttemptsInterval.hashCode() : 0);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FixedDelayRestartStrategyConfiguration) {
				FixedDelayRestartStrategyConfiguration other = (FixedDelayRestartStrategyConfiguration) obj;

				return restartAttempts == other.restartAttempts && delayBetweenAttemptsInterval.equals(other.delayBetweenAttemptsInterval);
			} else {
				return false;
			}
		}

		@Override
		public String getDescription() {
			return "Restart with fixed delay (" + delayBetweenAttemptsInterval + "). #"
				+ restartAttempts + " restart attempts.";
		}
	}

	/**
	 * Configuration representing a failure rate restart strategy.
	 */
	public static final class FailureRateRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 1195028697539661739L;
		private final int maxFailureRate;

		private final Time failureInterval;
		private final Time delayBetweenAttemptsInterval;

		public FailureRateRestartStrategyConfiguration(int maxFailureRate, Time failureInterval, Time delayBetweenAttemptsInterval) {
			this.maxFailureRate = maxFailureRate;
			this.failureInterval = failureInterval;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getMaxFailureRate() {
			return maxFailureRate;
		}

		public Time getFailureInterval() {
			return failureInterval;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public String getDescription() {
			return "Failure rate restart with maximum of " + maxFailureRate + " failures within interval " + failureInterval.toString()
					+ " and fixed delay " + delayBetweenAttemptsInterval.toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			FailureRateRestartStrategyConfiguration that = (FailureRateRestartStrategyConfiguration) o;
			return maxFailureRate == that.maxFailureRate &&
				Objects.equals(failureInterval, that.failureInterval) &&
				Objects.equals(delayBetweenAttemptsInterval, that.delayBetweenAttemptsInterval);
		}

		@Override
		public int hashCode() {
			return Objects.hash(maxFailureRate, failureInterval, delayBetweenAttemptsInterval);
		}
	}

	/**
	 * cluster 名词 串 簇 群
	 * Restart strategy configuration that could be used by jobs to use cluster level restart
	 * strategy. Useful especially when one has a custom implementation of restart strategy set via
	 * flink-conf.yaml.
	 */
	public static final class FallbackRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -4441787204284085544L;

		@Override
		public String getDescription() {
			return "Cluster level default restart strategy";
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			return o instanceof FallbackRestartStrategyConfiguration;
		}

		@Override
		public int hashCode() {
			return Objects.hash();
		}
	}
}
