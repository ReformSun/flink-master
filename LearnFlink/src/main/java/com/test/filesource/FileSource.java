package com.test.filesource;

import com.test.util.URLUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileSource<T> extends RichSourceFunction<T> implements CheckpointedFunction {
	private String path;
	private DeserializationSchema<T> deserializationS;
	private ValueState<Integer> valueState;
	// 开始读取位置
	private Integer line;
	private int readLine = 0;

	public FileSource() {
		path = URLUtil.baseUrl + "source.txt";
	}
	public FileSource(String path) {
		this.path = path;
	}

	public FileSource(String path, DeserializationSchema<T> deserializationS) {
		this.path = path;
		this.deserializationS = deserializationS;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null){
				if (line.equals(""))break;
				ctx.collect(deserializationS.deserialize(line.getBytes()));
				readLine++;
			}
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
//		KeyedStateStore stateStore = context.getKeyedStateStore();
//		valueState = stateStore.getState(new ValueStateDescriptor<Integer>("line", Types.INT()));
//		if (context.isRestored()){
//			line = valueState.value();
//		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		valueState.update(readLine);
	}

	@Override
	public void cancel() {

	}

}
