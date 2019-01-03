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

package org.apache.flink.cep.nfa;

/**
 * Set of actions when doing a state transition from a {@link State} to another.
 */
public enum StateTransitionAction {
	TAKE, // take the current event and assign it to the current state 处理当前事件并分配它给当前的状态
	IGNORE, // ignore the current event 忽略当前的事件
	PROCEED // do the state transition and keep the current event for further processing (epsilon transition) 执行状态转换并保留当前事件以进行进一步处理
}
