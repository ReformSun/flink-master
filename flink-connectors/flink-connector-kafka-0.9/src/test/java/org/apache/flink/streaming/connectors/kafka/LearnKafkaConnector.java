package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import static org.mockito.Mockito.mock;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LearnKafkaConnector extends TestLogger {

	@Mock
	SourceFunction.SourceContext sourceContext;
	@Mock
	AssignerWithPunctuatedWatermarks assignerWithPunctuatedWatermarks;




	@Test
	public void testMethod1() {

		sourceContext = mock(SourceFunction.SourceContext.class);
		Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets = new HashMap<>();
		assignerWithPunctuatedWatermarks = mock(AssignerWithPunctuatedWatermarks.class);
		try {
			SerializedValue<AssignerWithPeriodicWatermarks<Tuple2>> watermarksPeriodic = new SerializedValue(assignerWithPunctuatedWatermarks);


		} catch (IOException e) {
			e.printStackTrace();
		}
//		Kafka09Fetcher kafka09Fetcher = new Kafka09Fetcher();


	}
}
