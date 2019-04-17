package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.mockito.Matchers.any;

public class Queue {

	@Test
	public void testMethod2(){
		int totalNoOfKeyGroups = 100;
		int startKeyGroupIdx = 0;
		int endKeyGroupIdx = totalNoOfKeyGroups - 1; // we have 0 to 99
		final KeyGroupRange keyGroupRange = new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);
		final PriorityQueueSetFactory priorityQueueSetFactory = new HeapPriorityQueueSetFactory(keyGroupRange, totalNoOfKeyGroups, 128);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
		TimerSerializer<Integer,String> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer,String>> queue = priorityQueueSetFactory.create("__test_event_timers",timerSerializer);
		for (int i = 0; i < 100; i++) {
			TimerHeapInternalTimer<Integer, String> timer = new TimerHeapInternalTimer<>(10 + i, i, "hello_world_" + i);
			queue.add(timer);
		}
		long time = 15;

		System.out.println(queue.size() + "_start");
//		queue.bulkPoll((timer)->(true),(timer)->{
//			System.out.println(timer.getTimestamp());
//		});
				queue.bulkPoll((timer)->(timer.getTimestamp() <= time),(timer)->{
			System.out.println(timer.getTimestamp());
		});
		System.out.println(queue.size() + "_end");
	}
	@Test
	public void testMethod3(){
		int totalNoOfKeyGroups = 100;
		int startKeyGroupIdx = 0;
		int endKeyGroupIdx = totalNoOfKeyGroups - 1; // we have 0 to 99
		final KeyGroupRange keyGroupRange = new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);
		final PriorityQueueSetFactory priorityQueueSetFactory = new HeapPriorityQueueSetFactory(keyGroupRange, totalNoOfKeyGroups, 128);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
		TimerSerializer<Integer,String> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer,String>> queue = priorityQueueSetFactory.create("__test_event_timers",timerSerializer);
		for (int i = 0; i < 100; i++) {
			TimerHeapInternalTimer<Integer, String> timer = new TimerHeapInternalTimer<>(10 + i, i, "hello_world_" + i);
			queue.add(timer);
		}
		System.out.println(queue.size() + "_start");
		System.out.println(queue.peek().getTimestamp());
		System.out.println(queue.size() + "_end");
	}
}
