package com.test.filesource;

import com.test.util.MetricWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSourceBase<T> extends RichParallelSourceFunction<T> implements
	ResultTypeQueryable<T>,CheckpointListener {
	private DeserializationSchema<T> deserializationS;
	private String path;
	private long interval = 0;
	private Counter sum = null;

	public FileSourceBase(DeserializationSchema<T> deserializationS, String path,long interval) {
		this.deserializationS = deserializationS;
		this.path = path;
		this.interval = interval;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("customSumSourceData");
		sum = metricGroup.counter("sum");
		System.out.println("IndexOfThisSubtask:" + getRuntimeContext().getIndexOfThisSubtask());
		System.out.println("NumberOfParallelSubtasks:" + getRuntimeContext().getNumberOfParallelSubtasks());
		super.open(parameters);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null){
				if (line.equals(""))break;
				T row = deserializationS.deserialize(line.getBytes());
				sum.inc();
				MetricWriter.writerFile(sum,"custommetric.txt");
				ctx.collect(row);
				if (interval > 0){
					Thread.sleep(interval);
				}
			}
		}
	}


	@Override
	public void cancel() {
		System.out.println("aa");
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationS.getProducedType();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		System.out.println(checkpointId);
	}
}
