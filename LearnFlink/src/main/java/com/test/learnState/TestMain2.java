package com.test.learnState;

import com.test.filesource.FileSourceTuple3;
import com.test.sink.CustomPrintTuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;

public class TestMain2 {
	private static String path = "/Users/apple/Desktop/state/savepointData/savepoint-6c7bd9-73bbcfafd18c";
	/**
	 * 自定义流执行环境则是从安全点启动
	 * {@link org.apache.flink.runtime.minicluster.MiniCluster}
	 */
	public static void main(String[] args) throws IOException {
		final CustomStreamEnvironment env = new CustomStreamEnvironment();
//		env.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(path));
		env.setParallelism(4);
		env.enableCheckpointing(6000);
//        env.setRestartStrategy();
		FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///Users/apple/Desktop/state/checkpointData").toUri(),new Path
			("file:///Users/apple/Desktop/state/savepointData").toUri());
		env.setStateBackend(new RocksDBStateBackend(fsStateBackend));
//        testMethod1(env);
        testMethod2(env);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(StreamExecutionEnvironment env) {
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.keyBy(0).sum(1).addSink(new CustomPrintTuple("test.txt"));
	}

	public static void testMethod2(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.keyBy(0).sum(1).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
//					throw new Exception("test");
				}else {
					System.out.println(value.toString());
				}
			}
		});
	}

	private void getSavepointRestoreSettings(){
		SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("");
	}
}
