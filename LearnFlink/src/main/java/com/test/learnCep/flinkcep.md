## 类作用分析
org.apache.flink.cep.pattern.Pattern 包含了用户自定义的模式匹配
org.apache.flink.cep.nfa.compiler.NFAFactory
org.apache.flink.cep.nfa.State 每一个State代表一个用户自定义的Pattern，比如：
```
Pattern<Event,Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getA().equals("a");
			}
		}).followedBy("second").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getB() == 50;
			}
		});
		timeOut(dataStream,pattern,"second");
		
		会生成3个State 分别为first second end
```
org.apache.flink.cep.nfa.ComputationState 保存真实的状态的名字和版本号，其实是在代表了一个State
## NFAFactory和NFA的生成
 NFAFactory类CEPOperatorUtils的createPatternStream方法中通过
 final NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling);
 创建，需要Pattern类的实例化对象
 NFA类会在AbstractKeyedCEPPatternOperator方法的open方法被调用时，通过nfaFactory类的createNFA方法调用
## 过程分析
 在这里分析的以处理时间
 当第一个元素被AbstractKeyedCEPPatternOperator类的processElement方法处理时，会首先调用getNFAState()方法，先从computationStates的
 状态后端获取，这时状态后端中没有NFAState,那么调用NFA的createInitialNFAState方法获取，
```
    Queue<ComputationState> startingStates = new LinkedList<>();
    // 遍历所有的用户自定义的状态，每一个State
     		for (State<T> state : states.values()) {
     		     // 如果窗台是开始窗台就增加到队列中
     			if (state.isStart()) {
     				startingStates.add(ComputationState.createStartState(state.getName()));
     			}
     		}
     		// 把所有的开始状态对象，生成NFAState
     		return new NFAState(startingStates);
     }
```
此时得到含有开始窗台的NFAState对象，然后调用
```
    private transient SharedBuffer<IN> partialMatches; 此时为空 因为还没有任何匹配产生
    private void processEvent(NFAState nfaState, IN event, long timestamp) throws Exception {
   		Collection<Map<String, List<IN>>> patterns =
   				nfa.process(partialMatches, nfaState, event, timestamp, afterMatchSkipStrategy);
   		processMatchedSequences(patterns, timestamp);
   	}
```
然后先把event, timestamp, sharedBuffer包装成EventWrapper类然后调用doProcess方法
## NFA.doProcess方法分析
```
    private Collection<Map<String, List<T>>> doProcess(
   			final SharedBuffer<T> sharedBuffer,
   			final NFAState nfaState,
   			final EventWrapper event,
   			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {
   
   		final PriorityQueue<ComputationState> newPartialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
   		final PriorityQueue<ComputationState> potentialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
   
   		// iterate over all current computations 遍历当前的所有计算 当第一个元素过来时，PartialMatches中只有开始start状态的State
   		for (ComputationState computationState : nfaState.getPartialMatches()) {
   			// 计算下一个状态 当第一个元素过来时，sharedBuffer会为空的computationState代表了开始状态的State
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
```
## NFA.computeNextStates 方法分析
```
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
        		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
        		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
        		int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
        
        		final List<ComputationState> resultingComputationStates = new ArrayList<>();
        		// 得到所有边，进行遍历
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
        					final State<T> nextState = edge.getTargetState();
        					final State<T> currentState = edge.getSourceState();
        
        					final NodeId previousEntry = computationState.getPreviousBufferEntry();
        
        					final DeweyNumber currentVersion = computationState.getVersion().increase(takeBranchesToVisit);
        					final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
        					takeBranchesToVisit--;
        
        					final NodeId newEntry = sharedBuffer.put(
        						currentState.getName(),
        						event.getEventId(),
        						previousEntry,
        						currentVersion);
        
        					final long startTimestamp;
        					final EventId startEventId;
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
        
        		if (isStartState(computationState)) {
        			int totalBranches = calculateIncreasingSelfState(
        					outgoingEdges.getTotalIgnoreBranches(),
        					outgoingEdges.getTotalTakeBranches());
        
        			DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
        			ComputationState startState = ComputationState.createStartState(computationState.getCurrentStateName(), startVersion);
        			resultingComputationStates.add(startState);
        		}
        
        		if (computationState.getPreviousBufferEntry() != null) {
        			// release the shared entry referenced by the current computation state.
        			sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());
        		}
        
        		return resultingComputationStates;
        	}
```


## NFA.createDecisionGraph方法分析
```
// 当第一个元素过来时，computationState代表了开始状态的State 一个用户自定义状态，可以有多个 输出边一个State可能会有对个输出边的
private OutgoingEdges<T> createDecisionGraph(
			ConditionContext<T> context,
			ComputationState computationState,
			T event) {
		// 通过ComputationState得到真实的State
		State<T> state = getState(computationState);
		// 根据状态创建转出边
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);
		// 创建堆
		final Stack<State<T>> states = new Stack<>();
		// 把当前状态值放入堆容器中
		states.push(state);

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
		    // 获取栈顶的State
			State<T> currentState = states.pop();
			// 获取当前状态的状态转换类
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state 校验每个状态的所有状态过渡
			// 逻辑分析：如果其实状态是Take直接返回把当前状态转换增加到输出边中
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

```

 
