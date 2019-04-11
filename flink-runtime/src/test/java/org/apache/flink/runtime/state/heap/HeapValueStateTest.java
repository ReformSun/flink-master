package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.ArrayList;

/**
 *
 * {@link org.apache.flink.runtime.state.heap.HeapValueState}
* HeapValueState Tester. 
* 
* @author <Authors name> 
* @since <pre>四月 11, 2019</pre> 
* @version 1.0 
*/ 
public class HeapValueStateTest {

	CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable;

@Before
public void before() throws Exception {
	RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
		new RegisteredKeyValueStateBackendMetaInfo<>(
			StateDescriptor.Type.UNKNOWN,
			"test",
			IntSerializer.INSTANCE,
			new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

	MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>(IntSerializer.INSTANCE,new KeyGroupRange(0,10));
	keyContext.setKey(1);

	stateTable =
		new CopyOnWriteStateTable<>(keyContext, metaInfo);
} 

@After
public void after() throws Exception { 
} 

@Test
public void testMethod2(){
	ArrayList<Integer> state_1_1 = new ArrayList<>();
	state_1_1.add(41);
	stateTable.put(11,1,state_1_1);
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

	static class MockInternalKeyContext<T> implements InternalKeyContext<T> {

		private T key;
		private final TypeSerializer<T> serializer;
		private final KeyGroupRange keyGroupRange;

		public MockInternalKeyContext(TypeSerializer<T> serializer,KeyGroupRange keyGroupRange) {
			this.serializer = serializer;
			this.keyGroupRange = keyGroupRange == null ? new KeyGroupRange(0, 0) : keyGroupRange;
		}

		public void setKey(T key) {
			this.key = key;
		}

		@Override
		public T getCurrentKey() {
			return key;
		}

		@Override
		public int getCurrentKeyGroupIndex() {
			return 0;
		}

		@Override
		public int getNumberOfKeyGroups() {
			return 1;
		}

		@Override
		public KeyGroupRange getKeyGroupRange() {
			return keyGroupRange;
		}

		@Override
		public TypeSerializer<T> getKeySerializer() {
			return serializer;
		}
	}


} 
