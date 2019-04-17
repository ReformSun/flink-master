package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class LearnHeapValueState {
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
		HeapValueState heapValueState = new HeapValueState<Integer, Integer, ArrayList<Integer>>(
			stateTable,
			IntSerializer.INSTANCE,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			new ArrayList<Integer>());
		heapValueState.setCurrentNamespace(1);
		heapValueState.update(22);
	}
}
