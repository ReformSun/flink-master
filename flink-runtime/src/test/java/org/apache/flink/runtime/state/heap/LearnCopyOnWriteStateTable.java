package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.stream.Stream;

public class LearnCopyOnWriteStateTable {
	private CopyOnWriteStateTableTest.MockInternalKeyContext<Integer> keyContext;
	@Test
	public void testMethod1(){
		// 生成状态表
		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable = getStateTable();
		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(41);
		stateTable.put(1,2,state_1_1);

		StateTransformationFunction<ArrayList<Integer>, Integer> function =
			new StateTransformationFunction<ArrayList<Integer>, Integer>() {
				@Override
				public ArrayList<Integer> apply(ArrayList<Integer> previousState, Integer value) throws Exception {
					previousState.add(value);
					return previousState;
				}
			};
		try {
			stateTable.transform(1,2,3333,function);
			state_1_1 = function.apply(state_1_1,3333);
			ArrayList<Integer> integers = stateTable.get(1,2);
			Assert.assertEquals(state_1_1,integers);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testMethod2(){
		CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable = getStateTable();
		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(41);
		stateTable.put(1,2,state_1_1);
		Assert.assertEquals(1,state_1_1.size());
		state_1_1.add(44);
		ArrayList<Integer> integers = stateTable.get(1,2);
		Assert.assertEquals(2,integers.size());
	}
	@Test
	public void testMethod3(){
		CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable = getStateTable();
		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(41);
//		stateTable.put(1,1,state_1_1);
//		stateTable.put(null,null,state_1_1);
		stateTable.put(1,1,state_1_1);
		Assert.assertEquals(state_1_1,stateTable.get(1,1));
	}

	/**
	 * 测试key不同namespace相同
	 * 取值必须指定
	 * key
	 * namespace
	 */
	@Test
	public void testMethod4(){
		CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable = getStateTable();
		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(11);
		ArrayList<Integer> state_1_2 = new ArrayList<>();
		state_1_2.add(11);
		ArrayList<Integer> state_1_3 = new ArrayList<>();
		state_1_3.add(11);
		stateTable.put(1,1,state_1_1);
		stateTable.put(2,1,state_1_1);
		stateTable.put(3,1,state_1_1);
//		keyContext.setKey(1);
//		stateTable.get(1);
		Stream<Integer> stream = stateTable.getKeys(1);
		System.out.println(stream.count());

	}


	private CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> getStateTable(){
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		keyContext = new CopyOnWriteStateTableTest.MockInternalKeyContext<>(IntSerializer.INSTANCE);

		// 生成状态表
		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);
		return stateTable;
	}
}
