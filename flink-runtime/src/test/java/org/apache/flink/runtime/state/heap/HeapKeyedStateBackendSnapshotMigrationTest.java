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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.SerializableObject;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.RunnableFuture;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests backwards compatibility in the serialization format of heap-based KeyedStateBackends.
 */
public class HeapKeyedStateBackendSnapshotMigrationTest extends HeapStateBackendTestBase {

	@Test
	public void testMethod1() throws Exception{
//		KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
//		List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
//		final KeyedStateHandle serializedKeyGroupStates = generateKeyGroupState(keyGroupRange, testStates);
//		SnapshotResult<KeyedStateHandle> stateHandleSnapshotResult = new SnapshotResult<>(serializedKeyGroupStates,null);
//		System.out.println("dd");

	    try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()){
			final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());
//			keyedBackend.restore(StateObjectCollection.singleton(stateHandleSnapshotResult.getJobManagerOwnedSnapshot()));
			InternalMapState<String, Integer, Long, Long> state = keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);
			keyedBackend.setCurrentKey("abc");
			final Integer namespace1 = 1;
			final Integer namespace2 = 2;
			final Integer namespace3 = 3;
			state.setCurrentNamespace(namespace1);
			state.put(111L,333L);
			state.put(112L,334L);
			state.put(113L,335L);
			state.put(114L,336L);
			assertEquals(333L, (long) state.get(111L));
			assertEquals(334L, (long) state.get(112L));
			assertEquals(335L, (long) state.get(113L));
			assertEquals(336L, (long) state.get(114L));
			state.setCurrentNamespace(namespace2);
			state.put(111L,333L);
			assertEquals(333L, (long) state.get(111L));

			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
				1L,
				1L,
				new MemCheckpointStreamFactory(4 * 1024 * 1024),
				CheckpointOptions.forCheckpointWithDefaultLocation());

			snapshot.run();

			SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();

			byte[] bytes = InstantiationUtil.serializeObject(snapshotResult);
			File file = new File("/Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/testFlink.snapshot");
			file.createNewFile();
			Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);

			System.out.println("aaa");
		}
	}

	@Test
	public void testMethod3() throws Exception{
		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
			final Integer namespace1 = 1;
			final Integer namespace2 = 2;
			final Integer namespace3 = 3;
			File file = new File("/Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/testFlink.snapshot");
			final SnapshotResult<KeyedStateHandle> stateHandles;
			try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(file)))) {
				stateHandles = InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			}

			final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

			keyedBackend.restore(StateObjectCollection.singleton(stateHandles.getJobManagerOwnedSnapshot()));

			InternalMapState<String, Integer, Long, Long> state = keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);
			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			assertEquals(333L, (long) state.get(111L));
			assertEquals(334L, (long) state.get(112L));
			assertEquals(335L, (long) state.get(113L));
			assertEquals(336L, (long) state.get(114L));

		}
	}

	@Test
	public  void testMethod2() throws Exception{
		ClassLoader cl = getClass().getClassLoader();
		URL resource = cl.getResource("heap_keyed_statebackend_1_5_map.snapshot");

		Preconditions.checkNotNull(resource, "Binary snapshot resource not found!");

		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
			final Integer namespace1 = 1;
			final Integer namespace2 = 2;
			final Integer namespace3 = 3;

			final SnapshotResult<KeyedStateHandle> stateHandles;
			try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(resource.getFile())))) {
				stateHandles = InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			}

			final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

			keyedBackend.restore(StateObjectCollection.singleton(stateHandles.getJobManagerOwnedSnapshot()));

			InternalMapState<String, Integer, Long, Long> state = keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			assertEquals(33L, (long) state.get(33L));
			assertEquals(55L, (long) state.get(55L));
			assertEquals(2, getStateSize(state));

			state.setCurrentNamespace(namespace2);
			assertEquals(22L, (long) state.get(22L));
			assertEquals(11L, (long) state.get(11L));
			assertEquals(2, getStateSize(state));

			state.setCurrentNamespace(namespace3);
			assertEquals(44L, (long) state.get(44L));
			assertEquals(1, getStateSize(state));
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
				1L,
				1L,
				new MemCheckpointStreamFactory(4 * 1024 * 1024),
				CheckpointOptions.forCheckpointWithDefaultLocation());

			snapshot.run();
		}
	}

	@Test
	public void testMapStateMigrationAfterHashMapSerRemoval() throws Exception {
		ClassLoader cl = getClass().getClassLoader();
		URL resource = cl.getResource("heap_keyed_statebackend_1_5_map.snapshot");

		Preconditions.checkNotNull(resource, "Binary snapshot resource not found!");

		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
			final Integer namespace1 = 1;
			final Integer namespace2 = 2;
			final Integer namespace3 = 3;

			final SnapshotResult<KeyedStateHandle> stateHandles;
			try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(resource.getFile())))) {
				stateHandles = InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			}

			final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

			keyedBackend.restore(StateObjectCollection.singleton(stateHandles.getJobManagerOwnedSnapshot()));

			InternalMapState<String, Integer, Long, Long> state = keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			assertEquals(33L, (long) state.get(33L));
			assertEquals(55L, (long) state.get(55L));
			assertEquals(2, getStateSize(state));

			state.setCurrentNamespace(namespace2);
			assertEquals(22L, (long) state.get(22L));
			assertEquals(11L, (long) state.get(11L));
			assertEquals(2, getStateSize(state));

			state.setCurrentNamespace(namespace3);
			assertEquals(44L, (long) state.get(44L));
			assertEquals(1, getStateSize(state));

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			assertEquals(11L, (long) state.get(11L));
			assertEquals(44L, (long) state.get(44L));
			assertEquals(2, getStateSize(state));

			state.setCurrentNamespace(namespace3);
			assertEquals(22L, (long) state.get(22L));
			assertEquals(55L, (long) state.get(55L));
			assertEquals(33L, (long) state.get(33L));
			assertEquals(3, getStateSize(state));

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			assertEquals(11L, (long) state.get(11L));
			assertEquals(22L, (long) state.get(22L));
			assertEquals(33L, (long) state.get(33L));
			assertEquals(44L, (long) state.get(44L));
			assertEquals(55L, (long) state.get(55L));
			assertEquals(5, getStateSize(state));

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace3);
			assertEquals(11L, (long) state.get(11L));
			assertEquals(22L, (long) state.get(22L));
			assertEquals(33L, (long) state.get(33L));
			assertEquals(44L, (long) state.get(44L));
			assertEquals(55L, (long) state.get(55L));
			assertEquals(5, getStateSize(state));

			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
					1L,
					1L,
					new MemCheckpointStreamFactory(4 * 1024 * 1024),
					CheckpointOptions.forCheckpointWithDefaultLocation());

			snapshot.run();
		}
	}

	private <K, N, UK, UV> int getStateSize(InternalMapState<K, N, UK, UV> mapState) throws Exception {
		int i = 0;
		for (Iterator<Map.Entry<UK, UV>> itt = mapState.iterator(); itt.hasNext(); i++, itt.next()) {}
		return i;
	}

//	/**
//	 * This code was used to create the binary file of the old version's (< 1.5) snapshot used by this test.
//   * If you need to recreate the binary, you can comment this out and run it.
//	 */
//	private void createBinarySnapshotWithMap() throws Exception {
//
//		final String pathToWrite = "/PATH/TO/WRITE";
//
//		final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
//		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());
//
//		final Integer namespace1 = 1;
//		final Integer namespace2 = 2;
//		final Integer namespace3 = 3;
//
//		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
//			InternalMapState<String, Integer, Long, Long> state = keyedBackend.createMapState(IntSerializer.INSTANCE, stateDescr);
//
//			keyedBackend.setCurrentKey("abc");
//			state.setCurrentNamespace(namespace1);
//			state.put(33L, 33L);
//			state.put(55L, 55L);
//
//			state.setCurrentNamespace(namespace2);
//			state.put(22L, 22L);
//			state.put(11L, 11L);
//
//			state.setCurrentNamespace(namespace3);
//			state.put(44L, 44L);
//
//			keyedBackend.setCurrentKey("def");
//			state.setCurrentNamespace(namespace1);
//			state.put(11L, 11L);
//			state.put(44L, 44L);
//
//			state.setCurrentNamespace(namespace3);
//			state.put(22L, 22L);
//			state.put(55L, 55L);
//			state.put(33L, 33L);
//
//			keyedBackend.setCurrentKey("jkl");
//			state.setCurrentNamespace(namespace1);
//			state.put(11L, 11L);
//			state.put(22L, 22L);
//			state.put(33L, 33L);
//			state.put(44L, 44L);
//			state.put(55L, 55L);
//
//			keyedBackend.setCurrentKey("mno");
//			state.setCurrentNamespace(namespace3);
//			state.put(11L, 11L);
//			state.put(22L, 22L);
//			state.put(33L, 33L);
//			state.put(44L, 44L);
//			state.put(55L, 55L);
//
//			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
//					0L,
//					0L,
//					new MemCheckpointStreamFactory(4 * 1024 * 1024),
//					CheckpointOptions.forCheckpointWithDefaultLocation());
//
//			snapshot.run();
//
//			try (BufferedOutputStream bis = new BufferedOutputStream(new FileOutputStream(pathToWrite))) {
//				InstantiationUtil.serializeObject(bis, snapshot.get());
//			}
//		}
//	}

	/**
	 * [FLINK-5979]
	 *
	 * <p>This test takes a snapshot that was created with Flink 1.2 and tries to restore it in master to check
	 * the backwards compatibility of the serialization format of {@link StateTable}s.
	 */
	@Test
	public void testRestore1_2ToMaster() throws Exception {

		ClassLoader cl = getClass().getClassLoader();
		URL resource = cl.getResource("heap_keyed_statebackend_1_2.snapshot");

		Preconditions.checkNotNull(resource, "Binary snapshot resource not found!");

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
			final KeyGroupsStateHandle stateHandle;
			try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(resource.getFile())))) {
				stateHandle = InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			}
			keyedBackend.restore(StateObjectCollection.singleton(stateHandle));
			final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

			InternalListState<String, Integer, Long> state = keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

			assertEquals(7, keyedBackend.numStateEntries());

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(33L, 55L), state.get());
			state.setCurrentNamespace(namespace2);
			assertEquals(asList(22L, 11L), state.get());
			state.setCurrentNamespace(namespace3);
			assertEquals(Collections.singletonList(44L), state.get());

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(11L, 44L), state.get());

			state.setCurrentNamespace(namespace3);
			assertEquals(asList(22L, 55L, 33L), state.get());

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(11L, 22L, 33L, 44L, 55L), state.get());

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace3);
			assertEquals(asList(11L, 22L, 33L, 44L, 55L), state.get());
		}
	}

//	/**
//	 * This code was used to create the binary file of the old version's snapshot used by this test. If you need to
//	 * recreate the binary, you can comment this out and run it.
//	 */
//	private void createBinarySnapshot() throws Exception {
//
//		final String pathToWrite = "/PATH/TO/WRITE";
//
//		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
//		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());
//
//		final Integer namespace1 = 1;
//		final Integer namespace2 = 2;
//		final Integer namespace3 = 3;
//
//		final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend();
//
//		try {
//			InternalListState<Integer, Long> state = keyedBackend.createListState(IntSerializer.INSTANCE, stateDescr);
//
//			keyedBackend.setCurrentKey("abc");
//			state.setCurrentNamespace(namespace1);
//			state.add(33L);
//			state.add(55L);
//
//			state.setCurrentNamespace(namespace2);
//			state.add(22L);
//			state.add(11L);
//
//			state.setCurrentNamespace(namespace3);
//			state.add(44L);
//
//			keyedBackend.setCurrentKey("def");
//			state.setCurrentNamespace(namespace1);
//			state.add(11L);
//			state.add(44L);
//
//			state.setCurrentNamespace(namespace3);
//			state.add(22L);
//			state.add(55L);
//			state.add(33L);
//
//			keyedBackend.setCurrentKey("jkl");
//			state.setCurrentNamespace(namespace1);
//			state.add(11L);
//			state.add(22L);
//			state.add(33L);
//			state.add(44L);
//			state.add(55L);
//
//			keyedBackend.setCurrentKey("mno");
//			state.setCurrentNamespace(namespace3);
//			state.add(11L);
//			state.add(22L);
//			state.add(33L);
//			state.add(44L);
//			state.add(55L);
//			RunnableFuture<KeyGroupsStateHandle> snapshot = keyedBackend.snapshot(
//					0L,
//					0L,
//					new MemCheckpointStreamFactory(4 * 1024 * 1024),
//					CheckpointOptions.forCheckpointWithDefaultLocation());
//
//			snapshot.run();
//
//			try (BufferedOutputStream bis = new BufferedOutputStream(new FileOutputStream(pathToWrite))) {
//				InstantiationUtil.serializeObject(bis, snapshot.get());
//			}
//
//		} finally {
//			keyedBackend.close();
//			keyedBackend.dispose();
//		}
//	}

	public static KeyGroupsStateHandle generateKeyGroupState(
		KeyGroupRange keyGroupRange,
		List<? extends Serializable> states) throws IOException {

		Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

		// 编码数据的偏移量
		Tuple2<byte[], List<long[]>> serializedDataWithOffsets =
			serializeTogetherAndTrackOffsets(Collections.<List<? extends Serializable>>singletonList(states));

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, serializedDataWithOffsets.f1.get(0));

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(
			String.valueOf(UUID.randomUUID()),
			serializedDataWithOffsets.f0);

		return new KeyGroupsStateHandle(keyGroupRangeOffsets, allSerializedStatesHandle);
	}

	// 编码偏移量  保存的是值和每个值的偏移量
	public static Tuple2<byte[], List<long[]>> serializeTogetherAndTrackOffsets(
		List<List<? extends Serializable>> serializables) throws IOException {

		// 偏移量
		List<long[]> offsets = new ArrayList<>(serializables.size());
		// 编码组值
		List<byte[]> serializedGroupValues = new ArrayList<>();

		int runningGroupsOffset = 0;
		for(List<? extends Serializable> list : serializables) {

			long[] currentOffsets = new long[list.size()];
			offsets.add(currentOffsets);

			for (int i = 0; i < list.size(); ++i) {
				currentOffsets[i] = runningGroupsOffset;
				// 把待编码的对象进行编码处理
				byte[] serializedValue = InstantiationUtil.serializeObject(list.get(i));
				serializedGroupValues.add(serializedValue);
				runningGroupsOffset += serializedValue.length;
			}
		}

		//write all generated values in a single byte array, which is index by groupOffsetsInFinalByteArray
		byte[] allSerializedValuesConcatenated = new byte[runningGroupsOffset];
		runningGroupsOffset = 0;
		for (byte[] serializedGroupValue : serializedGroupValues) {
			System.arraycopy(
				serializedGroupValue,
				0,
				allSerializedValuesConcatenated,
				runningGroupsOffset,
				serializedGroupValue.length);
			runningGroupsOffset += serializedGroupValue.length;
		}
		return new Tuple2<>(allSerializedValuesConcatenated, offsets);
	}
}
