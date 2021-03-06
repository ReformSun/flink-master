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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import static org.apache.flink.cep.nfa.MigrationUtils.deserializeComputationStates;

/**
 * 非确定性有限自动实现
 * Non-deterministic finite automaton implementation.
 *
 * <p>The {@link AbstractKeyedCEPPatternOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *  <li>emitted (success)</li>
 *  <li>discarded (patterns containing NOT)</li>
 *  <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @param <T> Type of the processed events
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA<T> {

	/**
	 * 一组所有有效的NFA状态 有NFACompiler返回
	 * 包含匹配名和匹配状态
	 * A set of all the valid NFA states, as returned by the
	 * {@link NFACompiler NFACompiler}.
	 * These are directly derived from the user-specified pattern.
	 */
	private final Map<String, State<T>> states;

	/**
	 * The length of a windowed pattern, as specified using the
	 * {@link org.apache.flink.cep.pattern.Pattern#within(Time)}  Pattern.within(Time)}
	 * method.
	 */
	private final long windowTime;

	/**
	 * A flag indicating if we want timed-out patterns (in case of windowed patterns)
	 * to be emitted ({@code true}), or silently discarded ({@code false}).
	 */
	private final boolean handleTimeout;

	public NFA(
			final Collection<State<T>> validStates,
			final long windowTime,
			final boolean handleTimeout) {
		this.windowTime = windowTime;
		this.handleTimeout = handleTimeout;
		this.states = loadStates(validStates);
	}

	private Map<String, State<T>> loadStates(final Collection<State<T>> validStates) {
		Map<String, State<T>> tmp = new HashMap<>(4);
		for (State<T> state : validStates) {
			tmp.put(state.getName(), state);
		}
		return Collections.unmodifiableMap(tmp);
	}

	@VisibleForTesting
	public Collection<State<T>> getStates() {
		return states.values();
	}

	public NFAState createInitialNFAState() {
		Queue<ComputationState> startingStates = new LinkedList<>();
		for (State<T> state : states.values()) {
			if (state.isStart()) {
				startingStates.add(ComputationState.createStartState(state.getName()));
			}
		}
		return new NFAState(startingStates);
	}

	private State<T> getState(ComputationState state) {
		// 通过匹配模式名获取
		return states.get(state.getCurrentStateName());
	}

	// 判断状态是否是起点
	private boolean isStartState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStart();
	}

	private boolean isStopState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStop();
	}

	private boolean isFinalState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isFinal();
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState The NFAState object that we need to affect while processing
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Map<String, List<T>>> process(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final T event,
			final long timestamp) throws Exception {
		return process(sharedBuffer, nfaState, event, timestamp, AfterMatchSkipStrategy.noSkip());
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState The NFAState object that we need to affect while processing
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @param afterMatchSkipStrategy The skip strategy to use after per match
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Map<String, List<T>>> process(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final T event,
			final long timestamp,
			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {
		try (EventWrapper eventWrapper = new EventWrapper(event, timestamp, sharedBuffer)) {
			return doProcess(sharedBuffer, nfaState, eventWrapper, afterMatchSkipStrategy);
		}
	}

	/**
	 * 没有时间的时间戳低于给的那个
	 * Prunes states assuming there will be no events with timestamp <b>lower</b> than the given one.
	 * 清理共享缓存区和发射全部的延迟部分匹配
	 * It cleares the sharedBuffer and also emits all timed out partial matches.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState     The NFAState object that we need to affect while processing
	 * @param timestamp    timestamp that indicates that there will be no more events with lower timestamp
	 * @return all timed outed partial matches
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Tuple2<Map<String, List<T>>, Long>> advanceTime(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final long timestamp) throws Exception {

		final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();
		final PriorityQueue<ComputationState> newPartialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		Map<EventId, T> eventsCache = new HashMap<>();
		// 遍历nfa状态的部分匹配
		for (ComputationState computationState : nfaState.getPartialMatches()) {
			// 判断当前状态是否是延迟
			if (isStateTimedOut(computationState, timestamp)) {

				if (handleTimeout) {
					// extract the timed out event pattern
					Map<String, List<T>> timedOutPattern = sharedBuffer.materializeMatch(extractCurrentMatches(
						sharedBuffer,
						computationState), eventsCache);
					timeoutResult.add(Tuple2.of(timedOutPattern, timestamp));
				}

				sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());

				nfaState.setStateChanged();
			} else {
				newPartialMatches.add(computationState);
			}
		}

		nfaState.setNewPartialMatches(newPartialMatches);

		sharedBuffer.advanceTime(timestamp);

		return timeoutResult;
	}

	// 判断状态是否超时
	private boolean isStateTimedOut(final ComputationState state, final long timestamp) {
		// 判断条件是如果如果状态不是起点并且窗口时间大于零并且时间戳减去状态的开始时间戳大于或者等于窗口时间则为超时状态
		return !isStartState(state) && windowTime > 0L && timestamp - state.getStartTimestamp() >= windowTime;
	}

	private Collection<Map<String, List<T>>> doProcess(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final EventWrapper event,
			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {

		final PriorityQueue<ComputationState> newPartialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
		final PriorityQueue<ComputationState> potentialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		// iterate over all current computations 遍历当前的所有计算
		for (ComputationState computationState : nfaState.getPartialMatches()) {
			// 计算下一个状态
			final Collection<ComputationState> newComputationStates = computeNextStates(
				sharedBuffer,
				computationState,
				event,
				event.getTimestamp());

			if (newComputationStates.size() != 1) {
				nfaState.setStateChanged();
			} else if (!newComputationStates.iterator().next().equals(computationState)) {
				nfaState.setStateChanged();
			}

			//delay adding new computation states in case a stop state is reached and we discard the path.
			final Collection<ComputationState> statesToRetain = new ArrayList<>();
			//if stop state reached in this path
			boolean shouldDiscardPath = false;
			for (final ComputationState newComputationState : newComputationStates) {
				// 判断是否是结束状态
				if (isFinalState(newComputationState)) {
					//如果新的ComputationState到达最终态，则提取出其对应的事件匹配，并加入到待返回的结果集中
					potentialMatches.add(newComputationState);
				} else if (isStopState(newComputationState)) {
					//reached stop state. release entry for the stop state 到达stop状态时，释放停止状态的实体
					shouldDiscardPath = true;
					sharedBuffer.releaseNode(newComputationState.getPreviousBufferEntry());// 释放节点
				} else {
					// add new computation state; it will be processed once the next event arrives 增加新的计算状态
					statesToRetain.add(newComputationState);
				}
			}

			if (shouldDiscardPath) {
				// a stop state was reached in this branch. release branch which results in removing previous event from
				// the buffer
				for (final ComputationState state : statesToRetain) {
					sharedBuffer.releaseNode(state.getPreviousBufferEntry());
				}
			} else {
				newPartialMatches.addAll(statesToRetain);
			}
		}

		if (!potentialMatches.isEmpty()) {
			nfaState.setStateChanged();
		}

		List<Map<String, List<T>>> result = new ArrayList<>();
		if (afterMatchSkipStrategy.isSkipStrategy()) {
			processMatchesAccordingToSkipStrategy(sharedBuffer,
				nfaState,
				afterMatchSkipStrategy,
				potentialMatches,
				newPartialMatches,
				result);
		} else {
			for (ComputationState match : potentialMatches) {
				Map<EventId, T> eventsCache = new HashMap<>();
				// 获取当前部分匹配，所匹配到的值
				Map<String, List<T>> materializedMatch =
					sharedBuffer.materializeMatch(
						sharedBuffer.extractPatterns(
							match.getPreviousBufferEntry(),
							match.getVersion()).get(0),
						eventsCache
					);
				// 增加到结果集中
				result.add(materializedMatch);
				// 减少对前一个节点id的引用计数
				sharedBuffer.releaseNode(match.getPreviousBufferEntry());
			}
		}

		// 在nfa状态中设置新的部分匹配
		nfaState.setNewPartialMatches(newPartialMatches);

		return result;
	}

	private void processMatchesAccordingToSkipStrategy(
			SharedBuffer<T> sharedBuffer,
			NFAState nfaState,
			AfterMatchSkipStrategy afterMatchSkipStrategy,
			PriorityQueue<ComputationState> potentialMatches,
			PriorityQueue<ComputationState> partialMatches,
			List<Map<String, List<T>>> result) throws Exception {

		nfaState.getCompletedMatches().addAll(potentialMatches);

		ComputationState earliestMatch = nfaState.getCompletedMatches().peek();

		if (earliestMatch != null) {

			Map<EventId, T> eventsCache = new HashMap<>();
			ComputationState earliestPartialMatch;
			while (earliestMatch != null && ((earliestPartialMatch = partialMatches.peek()) == null ||
				isEarlier(earliestMatch, earliestPartialMatch))) {

				nfaState.setStateChanged();
				nfaState.getCompletedMatches().poll();
				List<Map<String, List<EventId>>> matchedResult =
					sharedBuffer.extractPatterns(earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());

				afterMatchSkipStrategy.prune(
					partialMatches,
					matchedResult,
					sharedBuffer);

				afterMatchSkipStrategy.prune(
					nfaState.getCompletedMatches(),
					matchedResult,
					sharedBuffer);

				result.add(sharedBuffer.materializeMatch(matchedResult.get(0), eventsCache));
				earliestMatch = nfaState.getCompletedMatches().peek();
			}

			nfaState.getPartialMatches().removeIf(pm -> pm.getStartEventID() != null && !partialMatches.contains(pm));
		}
	}

	private boolean isEarlier(ComputationState earliestMatch, ComputationState earliestPartialMatch) {
		return NFAState.COMPUTATION_STATE_COMPARATOR.compare(earliestMatch, earliestPartialMatch) <= 0;
	}

	private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
		return s1.getName().equals(s2.getName());
	}

	/**
	 * 存储已经解析装换的类
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
	private static class OutgoingEdges<T> {
		private List<StateTransition<T>> edges = new ArrayList<>();

		private final State<T> currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State<T> currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition<T> edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}

		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition<T>> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition<T> edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}

	/**
	 * Helper class that ensures event is registered only once throughout the life of this object and released on close
	 * of this object. This allows to wrap whole processing of the event with try-with-resources block.
	 */
	private class EventWrapper implements AutoCloseable {

		private final T event;

		private long timestamp;

		private final SharedBuffer<T> sharedBuffer;

		private EventId eventId;

		EventWrapper(T event, long timestamp, SharedBuffer<T> sharedBuffer) {
			this.event = event;
			this.timestamp = timestamp;
			this.sharedBuffer = sharedBuffer;
		}

		EventId getEventId() throws Exception {
			if (eventId == null) {
				this.eventId = sharedBuffer.registerEvent(event, timestamp);
			}

			return eventId;
		}

		T getEvent() {
			return event;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public void close() throws Exception {
			if (eventId != null) {
				sharedBuffer.releaseEvent(eventId);
			}
		}
	}

	/**
	 * 根据指定的状态，当前事件，它的时间戳和内部状态机，计算下一个计算状态
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 * 有效转换个分支路径数
	 * 执行转换
	 * IGNORE 将一直保持指向前一个事件
	 *  特殊情况，不要为起始状态执行
	 * TAKE 将指向当前事件
	 *
	 *
	 *<ol>
	 *     <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}</li>
	 * 	   <li>Perform transitions:
	 * 	   	<ol>
	 *          <li>IGNORE (links in {@link SharedBuffer} will still point to the previous event)</li>
	 *          <ul>
	 *              <li>do not perform for Start State - special case</li>
	 *          	<li>if stays in the same state increase the current stage for future use with number of outgoing edges</li>
	 *          	<li>if after PROCEED increase current stage and add new stage (as we change the state)</li>
	 *          	<li>lock the entry in {@link SharedBuffer} as it is needed in the created branch</li>
	 *      	</ul>
	 *      	<li>TAKE (links in {@link SharedBuffer} will point to the current event)</li>
	 *          <ul>
	 *              <li>add entry to the shared buffer with version of the current computation state</li>
	 *              <li>add stage and then increase with number of takes for the future computation states</li>
	 *              <li>peek to the next state if it has PROCEED path to a Final State, if true create Final
	 *              ComputationState to emit results</li>
	 *          </ul>
	 *      </ol>
	 *     </li>
	 * 	   <li>Handle the Start State, as it always have to remain </li>
	 *     <li>Release the corresponding entries in {@link SharedBuffer}.</li>
	 *</ol>
	 *
	 * @param sharedBuffer The shared buffer that we need to change
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timestamp Timestamp of the current event
	 * @return Collection of computation states which result from the current one
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Collection<ComputationState> computeNextStates(
			final SharedBuffer<T> sharedBuffer,
			final ComputationState computationState,
			final EventWrapper event,
			final long timestamp) throws Exception {

		final ConditionContext<T> context = new ConditionContext<>(this, sharedBuffer, computationState);
		// 创建决断图 里面包含的是状态转换的行为为take类型的StateTransition
		final OutgoingEdges<T> outgoingEdges = createDecisionGraph(context, computationState, event.getEvent());

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition<T>> edges = outgoingEdges.getEdges();
		// 得到输出边中的Take状态的边的分支数减1和0的大小 也就是要被访问的分支的访问索引
		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
		// 得到输出边中的Ignore状态的边的分支数
		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
		// 总的被跳过的Take状态的分支总数
		int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

		final List<ComputationState> resultingComputationStates = new ArrayList<>();
		for (StateTransition<T> edge : edges) {
			switch (edge.getAction()) {
				case IGNORE: {
					// 如果不是起始状态
					if (!isStartState(computationState)) {
						final DeweyNumber version;
						// 判断边的目标状态和就算状态对应的状态是否是同一个
						if (isEquivalentState(edge.getTargetState(), getState(computationState))) {
							//Stay in the same state (it can be either looping one or singleton)
							final int toIncrease = calculateIncreasingSelfState(
								outgoingEdges.getTotalIgnoreBranches(),
								outgoingEdges.getTotalTakeBranches());
							version = computationState.getVersion().increase(toIncrease);
						} else {
							//IGNORE after PROCEED
							version = computationState.getVersion()
								.increase(totalTakeToSkip + ignoreBranchesToVisit)
								.addStage();
							ignoreBranchesToVisit--;
						}

						addComputationState(
							sharedBuffer,
							resultingComputationStates,
							edge.getTargetState(),
							computationState.getPreviousBufferEntry(),
							version,
							computationState.getStartTimestamp(),
							computationState.getStartEventID()
						);
					}
				}
				break;
				case TAKE:
					// 得到边对应的下一个状态State
					final State<T> nextState = edge.getTargetState();
					// 得到边对应的上一个状态State
					final State<T> currentState = edge.getSourceState();
					// 得到当前状态对应的NodeId
					final NodeId previousEntry = computationState.getPreviousBufferEntry();
					// 得到当前计算状态的版本号 版本号计算方法 当前版本号加上被访问的分支的索引
					final DeweyNumber currentVersion = computationState.getVersion().increase(takeBranchesToVisit);
					// 获得下一个dewey数
					final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
					takeBranchesToVisit--;
					// 把当前状态的名字，事件的id 前一个节点和当前版本号存入共享缓存区中
					final NodeId newEntry = sharedBuffer.put(
						currentState.getName(),
						event.getEventId(),
						previousEntry,
						currentVersion);

					final long startTimestamp;
					final EventId startEventId;
					// 判断状态是否是开始状态
					if (isStartState(computationState)) {
						startTimestamp = timestamp;
						startEventId = event.getEventId();
					} else {
						startTimestamp = computationState.getStartTimestamp();
						startEventId = computationState.getStartEventID();
					}

					addComputationState(
							sharedBuffer,
							resultingComputationStates,
							nextState,
							newEntry,
							nextVersion,
							startTimestamp,
							startEventId);

					//check if newly created state is optional (have a PROCEED path to Final state)
					final State<T> finalState = findFinalStateAfterProceed(context, nextState, event.getEvent());
					if (finalState != null) {
						addComputationState(
								sharedBuffer,
								resultingComputationStates,
								finalState,
								newEntry,
								nextVersion,
								startTimestamp,
								startEventId);
					}
					break;
			}
		}
		// 当前状态是否是开始状态
		if (isStartState(computationState)) {
			// 如果是：计算自己的总的出边数量
			int totalBranches = calculateIncreasingSelfState(
					outgoingEdges.getTotalIgnoreBranches(),
					outgoingEdges.getTotalTakeBranches());
			// 根据总的出边数计算出开始版本号
			DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
			// 创建开始计算状态
			ComputationState startState = ComputationState.createStartState(computationState.getCurrentStateName(), startVersion);
			resultingComputationStates.add(startState);
		}

		if (computationState.getPreviousBufferEntry() != null) {
			// release the shared entry referenced by the current computation state.
			sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());
		}

		return resultingComputationStates;
	}

	private void addComputationState(
			SharedBuffer<T> sharedBuffer,
			List<ComputationState> computationStates,
			State<T> currentState,
			NodeId previousEntry,
			DeweyNumber version,
			long startTimestamp,
			EventId startEventId) throws Exception {
		ComputationState computationState = ComputationState.createState(
				currentState.getName(), previousEntry, version, startTimestamp, startEventId);
		computationStates.add(computationState);

		sharedBuffer.lockNode(previousEntry);
	}

	/**
	 * 如果下一个状态State内的转换器状态是PROCEED并且当前时间满足转换要求，则查看当前转换器对应的下一个状态是否是
	 * Final状态，如果是返回当前转换器的下一个状态，如果不是继续查找
	 * @param context
	 * @param state
	 * @param event
	 * @return
	 */
	private State<T> findFinalStateAfterProceed(
			ConditionContext<T> context,
			State<T> state,
			T event) {
		final Stack<State<T>> statesToCheck = new Stack<>();
		statesToCheck.push(state);
		try {
			while (!statesToCheck.isEmpty()) {
				final State<T> currentState = statesToCheck.pop();
				for (StateTransition<T> transition : currentState.getStateTransitions()) {
					if (transition.getAction() == StateTransitionAction.PROCEED &&
							checkFilterCondition(context, transition.getCondition(), event)) {
						if (transition.getTargetState().isFinal()) {
							return transition.getTargetState();
						} else {
							statesToCheck.push(transition.getTargetState());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failure happened in filter function.", e);
		}

		return null;
	}

	private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
		return takeBranches == 0 && ignoreBranches == 0 ? 0 : ignoreBranches + Math.max(1, takeBranches);
	}

	/**
	 * 创建决策图
	 * 一个状态模式 可以有多个过滤条件
	 * 根据过滤条件可以获得符合条件的转换状态 每个装换状态包含它的下一个装换状态
	 * @param context
	 * @param computationState
	 * @param event
	 * @return
	 */
	private OutgoingEdges<T> createDecisionGraph(
			ConditionContext<T> context,
			ComputationState computationState,
			T event) {
		// 获得状态
		State<T> state = getState(computationState);
		// 根据状态创建转出边
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);
		// 创建堆
		final Stack<State<T>> states = new Stack<>();
		// 把当前状态值放入堆容器中
		states.push(state);

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			// 获取当前状态的状态转化类
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state 校验每个状态的所有状态过渡
			// 逻辑分析：如果其实状态是TAke直接返回吧当前状态转换增加到输出边中
			for (StateTransition<T> stateTransition : stateTransitions) {
				try {
					// 校验过滤条件 执行我们设置的过滤条件
					if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
						// filter condition is true 过滤条件为真
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it 如果状态是继续 把当前状态过渡的目标状态放进堆中
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new FlinkRuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}

	/**
	 * 执行用户自定义的过滤条件
	 * @param context
	 * @param condition IterativeCondition 用户自定义的过滤条件
	 * @param event
	 * @return
	 * @throws Exception
	 */
	private boolean checkFilterCondition(
			ConditionContext<T> context,
			IterativeCondition<T> condition,
			T event) throws Exception {
		return condition == null || condition.filter(event, context);
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param sharedBuffer The {@link SharedBuffer} from which to extract the matches
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Map<String, List<EventId>> extractCurrentMatches(
			final SharedBuffer<T> sharedBuffer,
			final ComputationState computationState) throws Exception {
		if (computationState.getPreviousBufferEntry() == null) {
			return new HashMap<>();
		}

		List<Map<String, List<EventId>>> paths = sharedBuffer.extractPatterns(
				computationState.getPreviousBufferEntry(),
				computationState.getVersion());

		if (paths.isEmpty()) {
			return new HashMap<>();
		}
		// for a given computation state, we cannot have more than one matching patterns.
		Preconditions.checkState(paths.size() == 1);

		return paths.get(0);
	}

	/**
	 * The context used when evaluating this computation state.
	 */
	private static class ConditionContext<T> implements IterativeCondition.Context<T> {

		/** The current computation state. */
		private ComputationState computationState;

		/**
		 * The matched pattern so far. A condition will be evaluated over this
		 * pattern. This is evaluated <b>only once</b>, as this is an expensive
		 * operation that traverses a path in the {@link SharedBuffer}.
		 */
		private Map<String, List<T>> matchedEvents;

		private NFA<T> nfa;

		private SharedBuffer<T> sharedBuffer;

		ConditionContext(
				final NFA<T> nfa,
				final SharedBuffer<T> sharedBuffer,
				final ComputationState computationState) {
			this.computationState = computationState;
			this.nfa = nfa;
			this.sharedBuffer = sharedBuffer;
		}

		@Override
		public Iterable<T> getEventsForPattern(final String key) throws Exception {
			Preconditions.checkNotNull(key);

			// the (partially) matched pattern is computed lazily when this method is called.
			// this is to avoid any overheads when using a simple, non-iterative condition.

			if (matchedEvents == null) {
				this.matchedEvents = sharedBuffer.materializeMatch(nfa.extractCurrentMatches(sharedBuffer,
					computationState));
			}

			return new Iterable<T>() {
				@Override
				public Iterator<T> iterator() {
					List<T> elements = matchedEvents.get(key);
					return elements == null
						? Collections.EMPTY_LIST.<T>iterator()
						: elements.iterator();
				}
			};
		}
	}

	////////////////////				DEPRECATED/MIGRATION UTILS

	/**
	 * Wrapper for migrated state.
	 */
	public static class MigratedNFA<T> {

		private final Queue<ComputationState> computationStates;
		private final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer;

		public org.apache.flink.cep.nfa.SharedBuffer<T> getSharedBuffer() {
			return sharedBuffer;
		}

		public Queue<ComputationState> getComputationStates() {
			return computationStates;
		}

		MigratedNFA(
				final Queue<ComputationState> computationStates,
				final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer) {
			this.sharedBuffer = sharedBuffer;
			this.computationStates = computationStates;
		}
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} serializer configuration to be stored with the managed state.
	 */
	@Deprecated
	public static final class NFASerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty constructor is required for deserializing the configuration. */
		public NFASerializerConfigSnapshot() {}

		public NFASerializerConfigSnapshot(
				TypeSerializer<T> eventSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {

			super(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	/**
	 * Only for backward compatibility with <=1.5.
	 */
	@Deprecated
	public static class NFASerializer<T> extends TypeSerializer<MigratedNFA<T>> {

		private static final long serialVersionUID = 2098282423980597010L;

		private final TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer;

		private final TypeSerializer<T> eventSerializer;

		public NFASerializer(TypeSerializer<T> typeSerializer) {
			this(typeSerializer,
				new org.apache.flink.cep.nfa.SharedBuffer.SharedBufferSerializer<>(
					StringSerializer.INSTANCE,
					typeSerializer));
		}

		NFASerializer(
				TypeSerializer<T> typeSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = sharedBufferSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public NFASerializer<T> duplicate() {
			return new NFASerializer<>(eventSerializer.duplicate());
		}

		@Override
		public MigratedNFA<T> createInstance() {
			return null;
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from, MigratedNFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(MigratedNFA<T> record, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> deserialize(DataInputView source) throws IOException {
			MigrationUtils.skipSerializedStates(source);
			source.readLong();
			source.readBoolean();

			org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer = sharedBufferSerializer.deserialize(source);
			Queue<ComputationState> computationStates = deserializeComputationStates(sharedBuffer, eventSerializer, source);

			return new MigratedNFA<>(computationStates, sharedBuffer);
		}

		@Override
		public MigratedNFA<T> deserialize(
				MigratedNFA<T> reuse,
				DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()) &&
					sharedBufferSerializer.equals(((NFASerializer) obj).sharedBufferSerializer) &&
					eventSerializer.equals(((NFASerializer) obj).eventSerializer));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new NFASerializerConfigSnapshot<>(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public CompatibilityResult<MigratedNFA<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof NFASerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
					((NFASerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T> eventCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					serializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					serializersAndConfigs.get(0).f1,
					eventSerializer);

				CompatibilityResult<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufCompatResult =
					CompatibilityUtil.resolveCompatibilityResult(
						serializersAndConfigs.get(1).f0,
						UnloadableDummyTypeSerializer.class,
						serializersAndConfigs.get(1).f1,
						sharedBufferSerializer);

				if (!sharedBufCompatResult.isRequiresMigration() && !eventCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else {
					if (eventCompatResult.getConvertDeserializer() != null &&
						sharedBufCompatResult.getConvertDeserializer() != null) {
						return CompatibilityResult.requiresMigration(
							new NFASerializer<>(
								new TypeDeserializerAdapter<>(eventCompatResult.getConvertDeserializer()),
								new TypeDeserializerAdapter<>(sharedBufCompatResult.getConvertDeserializer())));
					}
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}
}
