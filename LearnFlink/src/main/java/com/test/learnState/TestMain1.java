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
import org.apache.flink.api.common.functions.MapFunction;
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
 *
 * 检查点触发的过程
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}
 * CheckpointCoordinator检查点协调器他负责所有关于检查点的动作，包括触发检查点，触发安全点，从检查点恢复，触发查询请求
 * 每一个job任务有一个检查点协调器，每一个jobMater拥有一个ExecutionGraph{@link org.apache.flink.runtime.executiongraph.ExecutionGraph}
 * 而ExecutionGraph是一个job的执行图，实际的执行图，包含所有任务子任务的信息，也包含检查点协调器
 * 而{@link org.apache.flink.runtime.jobmaster.JobMaster}负责一个job的所有相关动作的执行，他会把与taskmananger的沟通信息
 * 在分发给不同工作的组件执行，也就是会把检查点相关的分发给CheckpointCoordinator，CheckpointCoordinator是会别定期执行的
 * 会调用startCheckpointScheduler()方法，开始检查点，时间间隔是可以设置的，安全点的触发是需要手动触发的
 *
 * 当触发检查点时，CheckpointCoordinator会调用triggerCheckpoint方法，这个方法会遍历job任务中的所有子任务Execution
 * {@link org.apache.flink.runtime.executiongraph.Execution}包含所有一个子任务信息，可以通过它与它被分配的taskmanager交流，
 * 触发他的方法triggerCheckpoint，这个方法会向taskmanager发送触发检查点的请求
 *
 * {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}TaskExecutor得到触发检查点的请求后，会根据ExecutionAttemptID
 * {@link org.apache.flink.runtime.executiongraph.ExecutionAttemptID}获取对应的正在执行的任务，然后调用task的triggerCheckpointBarrier方法
 *
 * {@link org.apache.flink.runtime.taskmanager.Task} 每一个job任务的子任务都会对应一个task，这个task负责实际运行的任务的
 * 与外界的交互工作，task中拥有真是执行的{@link org.apache.flink.streaming.runtime.tasks.StreamTask}类，比如StreamTask
 * task会调用StreamTask的triggerCheckpoint触发检查点的方法
 *
 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask}StreamTask他负责真实的执行逻辑，每一个子任务就会有一个这样的类
 * 当StreamTask的triggerCheckpoint方法被触发时，会再次调用performCheckpoint方法
 * 这时会执行三步
 * 第一步： 为做检查点准备，让全部的算子做预屏障的工作
 * 第二步：发送检查点屏障到下游，这时以广播的形式
 * 第三步：拍摄状态快照 实际调用的是内部类CheckpointingOperation，CheckpointingOperation负责具体的快照工作
 * 这个快照的工作已异步执行的交给他的内部类AsyncCheckpointRunnable
 *
 * 首先遍历这个子任务中所有的算子{@link org.apache.flink.streaming.api.operators.StreamOperator}实现这个接口的就是一个算子
 * 我们以{@link org.apache.flink.streaming.api.operators.StreamSink} 这就是一种类型的算子，当对算子的状态进行快照时，会调用
 * 算子的snapshotState方法一StreamSink为例，会把本算子的状态信息，存储到OperatorSnapshotFutures{@link org.apache.flink.streaming.api.operators.OperatorSnapshotFutures}
 * 中，然后根据算子的id把他们存储到Map中
 *
 * 执行完毕调用AsyncCheckpointRunnable的reportCompletedSnapshotStates方法，然后调用task的状态管理器{@link org.apache.flink.runtime.state.TaskStateManagerImpl}
 * 的reportTaskStateSnapshots方法，这个类拥有调用这次检查点的组件的网络地址信息，{@link org.apache.flink.runtime.taskmanager.ActorGatewayCheckpointResponder}
 * 他可以与让这个任务进行检查的服务联系，让后把检查点快照信息，返回给对应的组件。
 *
 * 也就是{@link org.apache.flink.runtime.jobmaster.JobMaster}的acknowledgeCheckpoint方法，然后调用{@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}
 * 的receiveAcknowledgeMessage方法，最后调用{@link org.apache.flink.runtime.checkpoint.PendingCheckpoint}的finalizeCheckpoint方法
 *
 */
public class TestMain1 {
	/**
	 * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment}
	 * {@link org.apache.flink.runtime.minicluster.MiniCluster}
	 */
	public static void main(String[] args) throws IOException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(4);
        env.enableCheckpointing(6000);
//        env.setRestartStrategy();
		FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///Users/apple/Desktop/state/checkpointData").toUri(),new Path
			("file:///Users/apple/Desktop/state/savepointData").toUri());
        env.setStateBackend(new RocksDBStateBackend(fsStateBackend));
//        testMethod1(env);
//		testMethod2(env);
		testMethod3(env);
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
	 * 在运行时的状态下不会引起任务的崩溃
	 *
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

	public static void testMethod3(StreamExecutionEnvironment env){
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.addSource(new FileSourceTuple3(1000)).setParallelism(1);
		dataStreamSource1.map(new MapFunction<Tuple3<String,Integer,Long>, Tuple3<String,Integer,Long>>() {
			@Override
			public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
				if (value != null){
					throw new IllegalArgumentException("testMap");
				}
				return value;
			}
		}).addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
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
