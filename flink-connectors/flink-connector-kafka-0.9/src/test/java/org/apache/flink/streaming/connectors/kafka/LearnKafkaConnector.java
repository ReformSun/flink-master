package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import static org.mockito.Mockito.mock;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
			TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
			KeyedDeserializationSchema<String> schema = new KeyedDeserializationSchemaWrapper<>(new SimpleStringSchema());

			Kafka09Fetcher kafka09Fetcher = new Kafka09Fetcher(sourceContext,assignedPartitionsWithInitialOffsets,watermarksPeriodic,
				null,testProcessingTimeService,
				10,
				this.getClass().getClassLoader(),
				null,
				schema,
				new Properties(),
				0,
			null,
				null,
				false);



		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Kafka09Fetcher kafka09Fetcher = new Kafka09Fetcher();


	}
}
