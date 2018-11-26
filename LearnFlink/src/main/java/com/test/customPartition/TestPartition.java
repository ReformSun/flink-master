package com.test.customPartition;

import org.apache.flink.api.common.functions.Partitioner;

public class TestPartition implements Partitioner<Long> {
	@Override
	public int partition(Long key, int numPartitions) {
		System.out.println(key + "dddd " + numPartitions);
		return 0;
	}
}
