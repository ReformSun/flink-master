package com.test.filesource;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.test.util.URLUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Types;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FileSourceTuple3 extends RichParallelSourceFunction<Tuple3<String,Integer,Long>> implements CheckpointedFunction {
	private String path;
	// 单位毫秒 调整数据发送速度
	private long interval = 0;
	// 开始读取位置
	private Integer line;
	private int readLine = 0;
	private ListState<Integer> listState;
	public FileSourceTuple3() {
		path = URLUtil.baseUrl + "source.txt";
	}

	public FileSourceTuple3(long interval) {
		this.interval = interval;
		path = URLUtil.baseUrl + "source.txt";
	}

	public FileSourceTuple3(String path) {
		this.path = path;
	}
	@Override
	public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null){
				List<String> list2 = Splitter.on(",").trimResults(CharMatcher.is('(').or(CharMatcher.is(')'))).splitToList(line);
				if (list2.size() == 3){
					Tuple3<String,Integer,Long> tuple3 = new Tuple3<>();
					tuple3.f0 = list2.get(0);
					tuple3.f1 = Integer.valueOf(list2.get(1));
					tuple3.f2 = Long.valueOf(list2.get(2));
					ctx.collect(tuple3);
				}
				readLine++;
				if (interval > 0){
					Thread.sleep(interval);
				}

			}
		}

	}
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		OperatorStateStore operatorStateStore = context.getOperatorStateStore();
		ListState<Integer> listStates = operatorStateStore.getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);
		listState = operatorStateStore.getUnionListState(new ListStateDescriptor<Integer>("line", Types.INT()));
		if (context.isRestored()){
			Iterator<Integer> integerIterator = listState.get().iterator();
			while (integerIterator.hasNext()){
				line = integerIterator.next();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		listState.update(Arrays.asList(readLine));
	}
	@Override
	public void cancel() {

	}
}
