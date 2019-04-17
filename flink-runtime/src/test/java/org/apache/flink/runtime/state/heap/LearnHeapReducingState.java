package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class LearnHeapReducingState {
	CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable;

	@Before
	public void before() throws Exception {
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		HeapValueStateTest.MockInternalKeyContext<Integer> keyContext = new HeapValueStateTest.MockInternalKeyContext<>(IntSerializer.INSTANCE,new KeyGroupRange(0,10));
		keyContext.setKey(1);

		stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);
	}
	@Test
	public void testMethod1(){
		HeapReducingState heapReducingState = new HeapReducingState(stateTable,IntSerializer.INSTANCE,stateTable.getStateSerializer(),stateTable.getNamespaceSerializer(),new ArrayList<Integer>(),new
			SumReducer());
		try {
			heapReducingState.setCurrentNamespace(1);
			heapReducingState.add(11);
			Assert.assertEquals(11,heapReducingState.get());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
											  Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}
}
