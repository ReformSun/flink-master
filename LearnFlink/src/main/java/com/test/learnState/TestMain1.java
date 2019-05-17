package com.test.learnState;

import com.test.customPartition.TestPartition;
import com.test.filesource.FileSourceTuple3;
import com.test.flatMap_1.SunFunctionStates1;
import com.test.learnWindows.KafkaUtil;
import com.test.sink.CustomPrintTuple;
import com.test.sink.CustomPrintTuple3;
import com.test.sink.CustomWordCountPrint;
import com.test.util.DataUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import test.SunWordWithCount;

import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

/**
 * 任务启动执行的流程
 * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment}
 * execute 87
 * {@link org.apache.flink.runtime.minicluster.MiniCluster}
 * executeJobBlocking 604
 * {@link org.apache.flink.runtime.dispatcher.Dispatcher}
 * submitJob 239
 * persistAndRunJob 266
 * runJob 282
 * createJobManagerRunner 292
 * {@link org.apache.flink.runtime.jobmaster.JobManagerRunner}
 * JobManagerRunner 初始化方法 105
 * new JobMaster 152
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * createAndRestoreExecutionGraph 1133
 *
 * 开始任务
 * {@link org.apache.flink.runtime.jobmaster.JobManagerRunner}
 * grantLeadership 307
 * verifyJobSchedulingStatusAndStartJobManager 322
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * start
 * 生成Execution的过程
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * createExecutionGraph
 * {@link  org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder}
 * buildGraph 124
 * {@link  org.apache.flink.runtime.executiongraph.ExecutionGraph}
 * attachJobGraph 811
 * {@link  org.apache.flink.runtime.executiongraph.ExecutionJobVertex}
 * ExecutionJobVertex 初始化方法 230
 * {@link org.apache.flink.runtime.executiongraph.ExecutionVertex}
 * ExecutionVertex 初始化方法 140 167
 * {@link org.apache.flink.runtime.executiongraph.Execution}
 * Execution 初始化
 *
 * 提交的过程 还有很多种
 * 一种
 * {@link org.apache.flink.runtime.executiongraph.ExecutionVertex}
 * deployToSlot 635 分配slot
 * {@link org.apache.flink.runtime.executiongraph.Execution}
 * deploy 536
 * 二种
 * {@link org.apache.flink.runtime.rest.handler.job.rescaling.RescalingHandlers}
 * triggerOperation
 * {@link org.apache.flink.runtime.dispatcher.Dispatcher}
 * rescaleJob 371
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * rescaleJob 410
 * rescaleOperators 424
 * scheduleExecutionGraph 1119
 * {@link  org.apache.flink.runtime.executiongraph.ExecutionGraph}
 * scheduleForExecution 860
 * scheduleEager 930
 * {@link org.apache.flink.runtime.executiongraph.Execution}
 * deploy 536
 * 594 final CompletableFuture<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, rpcTimeout);
 * 三种
 * {@link org.apache.flink.runtime.jobmaster.JobManagerRunner}
 * grantLeadership 307 答应给予领导权
 * verifyJobSchedulingStatusAndStartJobManager 322 开始任务管理
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId, final Time timeout)
 * startJobExecution(JobMasterId newJobMasterId)
 * resetAndScheduleExecutionGraph()
 * scheduleExecutionGraph()
 * {@link  org.apache.flink.runtime.executiongraph.ExecutionGraph}
 * scheduleForExecution 860
 * scheduleEager 930
 * {@link org.apache.flink.runtime.executiongraph.Execution}
 * deploy 536
 *
 *
 * 执行流程
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}
 * triggerCheckpoint
 * {@link org.apache.flink.runtime.checkpoint.CheckpointTriggerResult}
 * 的到触发检查点的结果 有成功和失败
 * {@link org.apache.flink.runtime.checkpoint.CheckpointDeclineReason} 失败
 * ####
 * 学习成功的流程
 * {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * acknowledgeCheckpoint
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}
 * receiveAcknowledgeMessage
 * completePendingCheckpoint
 * {@link org.apache.flink.runtime.checkpoint.PendingCheckpoint} 成功
 * finalizeCheckpoint 结束检查点方法
 * {@link org.apache.flink.runtime.checkpoint.Checkpoints}
 * storeCheckpointMetadata 调用保存检查点元数据
 * {@link org.apache.flink.runtime.state.filesystem.FsCheckpointMetadataOutputStream}
 * write 写元元素
 * #######
 * FsCheckpointMetadataOutputStream 这个类的生成
 * {@link org.apache.flink.runtime.state.filesystem.FsStateBackend}
 * createCheckpointStorage
 * {@link org.apache.flink.runtime.state.filesystem.FsCheckpointStorage}
 * 创建状态后端的checkpoint文件名字以jobid命名和三个子文件 chk- 、shared和taskowned
 * initializeLocationForCheckpoint
 * {@link org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation}
 * createMetadataOutputStream
 *
 * 执行一个完成的流程的检查点细节的地方可以重新查看
 *
 * {@link org.apache.flink.runtime.state.DefaultOperatorStateBackend}
 * snapshot
 * {@link org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources}
 * performOperation
 * {@link org.apache.flink.core.memory.DataOutputViewStreamWrapper}
 * {@link org.apache.flink.runtime.state.OperatorBackendSerializationProxy}
 * write
 * writeStateMetaInfoSnapshots
 *
 * 怎么和用户创建的状态后端联系到一起的
 * {@link org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources}
 * openOutStream
 * {@link org.apache.flink.runtime.state.CheckpointStreamFactory}
 * createCheckpointStateOutputStream
 * 如果是FS的状态后端
 * {@link org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation}
 * 继成自{@link org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory}
 * createCheckpointStateOutputStream
 * 这样就实现了和用户自定义状态后端的关联
 *
 * ## 状态后端恢复的过程
 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask}
 * initializeState
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * 或者{@link org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator}
 * initializeState
 * {@link org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl}
 * streamOperatorStateContext
 * {@link org.apache.flink.runtime.state.TaskStateManagerImpl}
 * prioritizedOperatorState
 * {@link org.apache.flink.streaming.api.operators.BackendRestorerProcedure}
 * createAndRestore
 * {@link org.apache.flink.runtime.state.DefaultOperatorStateBackend}
 * restore
 *
 *
 */
public class TestMain1 {
	/**
	 * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment}
	 * {@link org.apache.flink.runtime.minicluster.MiniCluster}
	 */
	public static void main(String[] args) throws IOException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

	/**
	 * 测试错误的信息
	 * @param env
	 */
	public static void testMethod2(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.keyBy(0).sum(1).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
					throw new Exception("test");
				}else {
					System.out.println(value.toString());
				}
			}
		});
	}

}
