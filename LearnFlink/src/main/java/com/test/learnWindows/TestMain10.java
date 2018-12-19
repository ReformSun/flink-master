package com.test.learnWindows;

import com.test.asyncFunction.CustomAsyncFunctonTuple3;
import com.test.asyncFunction.CustomAsyncFunctonTuple3_1;
import com.test.cache.CustomCacheDataBaseConf;
import com.test.cache.Key;
import com.test.map.CustomMapTuple3;
import com.test.sink.CustomPrintTuple3;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 操作算子：
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator
 * org.apache.flink.streaming.api.operators.StreamSink
 * org.apache.flink.streaming.api.operators.StreamMap
 * org.apache.flink.streaming.api.operators.StreamFilter
 * org.apache.flink.streaming.api.operators.StreamFlatMap
 * org.apache.flink.streaming.api.operators.StreamGroupedReduce
 * org.apache.flink.streaming.api.operators.ProcessOperator
 * org.apache.flink.streaming.api.operators.StoppableStreamSource
 * org.apache.flink.streaming.api.operators.KeyedProcessOperator
 * org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
 * 上面两个类是处理输入流元素的重要接口 操作者需要实现这两个中的一个接口
 * org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator
 * 用户自定义操作的重要接口 他实现了很多重要的接口 比如窗台初始化，快照，检查点，所有的一个操作的相关步骤都在这个类中
 * 举例：StreamSource操作只继承了AbstractUdfStreamOperator类 说明他只能发射数据不能接受数据
 * 而 StreamSink操作继承了AbstractUdfStreamOperator类和实现了OneInputStreamOperator的方法 说明它既能接受数据也能发送数据
 *
 * 每个任务的执行流程：
 * org.apache.flink.runtime.taskmanager.Task
 * org.apache.flink.streaming.runtime.tasks.StreamTask
 * org.apache.flink.streaming.runtime.tasks.SourceStreamTask
 * org.apache.flink.streaming.runtime.tasks.OneInputStreamTask
 * org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask
 * Task 是一个算子的执行线程的操作类继承自Runnable
 * 在Task的run方法中会根据算子的不同生成StreamTask，SourceStreamTask，OneInputStreamTask和TwoInputStreamTask不同的实例化对象
 * 然后调用他们的invoke方法初始化状态后端，key后端，定时器服务，初始化执行链，调用执行链中各个链节点的open方法配置他们
 * 各个链节点就是各个算子 具体的执行过程
 *
 * 数据实现了这些接口到底怎样发送和接受的那
 * org.apache.flink.streaming.api.functions.source.FromElementsFunction
 * org.apache.flink.streaming.api.operators.StreamSourceContexts
 * ManualWatermarkContext AutomaticWatermarkContext NonTimestampContext类都在org.apache.flink.streaming.api.operators.StreamSourceContexts类中
 * org.apache.flink.streaming.api.operators.Output
 * org.apache.flink.streaming.runtime.tasks.OperatorChain
 * org.apache.flink.streaming.runtime.io.RecordWriterOutput
 * source
 * 只继乘了AbstractUdfStreamOperator接口 它是怎样实现数据的发送的那
 * env.fromCollection(list)
 * 以FromElementsFunction为例；StreamSource包含用户自定义的userFunction方法
 * 当StreamSource被初始化和运行起来时，会被调用run方法，run方法会调用用户自定义的方法（首先有一个前提当StreamSource初始化时会初始化一个source上下文，这个上下文是会通过StreamSourceContexts类创建，
 * 这个上下文有三种种ManualWatermarkContext，AutomaticWatermarkContext和NonTimestampContext，根据选择系统处理时间决定）
 * 上下文对应时间类型关系
 * ManualWatermarkContext  EventTime 事件时间
 * AutomaticWatermarkContext IngestionTime 数据进入flink的时间
 * NonTimestampContext ProcessingTime 数据被处理的时间
 *
 * 每个上下文在初始化时，都拥有了StreamSource实例化对象的Output对象
 * Output对象的collect方法实现了发射数据记录的功能
 * Output对象的emitWatermark方法实现了发射水印的功能
 *
 * 而Output对象的实例化是在OperatorChain类构造方法中初始化的
 * this.chainEntryPoint = createOutputCollector(containingTask,configuration,chainedConfigs,userCodeClassloader,streamOutputMap,allOps);
 * streamOutputMap参数包含了每个输出边outEdge所对应的RecordWriterOutput实例化对象
 * RecordWriterOutput是对netty网络传输服务更高层次的封装，它可以直接进行和下一个子任务交互
 *
 *
 * 所以当我们在用户自定义方法userFunction中调用sourceContext的collect方法把数据传给sourceContext时，就会直接发送给下一个子任务，也就是调用Output.collect -》 RecordWriterOutput.collect -》RecordWriterOutput.pushToRecordWriter
 * 异步io
 *
 *
 *
 */
public class TestMain10 extends AbstractTestMain1 {

	public static void main(String[] args) {
		try{
//			testMethod1();
//			testMethod2();
			testMethod3();
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test10");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void testMethod1() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1);
		DataStream dataStream = dataStreamSource1.map(new CustomMapTuple3()).setParallelism(1);
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

	public static void testMethod2() {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1);
		AsyncWaitOperator asyncWaitOperator = new AsyncWaitOperator(new CustomAsyncFunctonTuple3(),11111,2222, AsyncDataStream.OutputMode.ORDERED);
		DataStream dataStream = dataStreamSource1.transform("test",new TypeHint<Tuple3<String,String,Long>>(){}.getTypeInfo(),asyncWaitOperator);
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

	public static void testMethod3() throws Exception {
		List<Tuple3<String,Integer,Long>> list = getTestdata();
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1);
		Map<String,Object> map = new HashMap<>();
//		设置数据库配置信息
		map.put("url","jdbc:postgresql://10.4.247.20:5432/apm_test");
		map.put("driver_class","org.postgresql.Driver");
		map.put("user","apm");
		map.put("password","apm");
		// 配置缓存池
		map.put("refreshinterval",10000L);
		// 设置表明
		map.put("tablename","flink_map");
		CustomCacheDataBaseConf customCacheDataBaseConf = new CustomCacheDataBaseConf();
		customCacheDataBaseConf.setDataBaseConf(map);
		customCacheDataBaseConf.setFirstKey(new Key("mapname","long"));
		customCacheDataBaseConf.setSecondKey(new Key("mapvalue","string"));
		AsyncWaitOperator asyncWaitOperator = new AsyncWaitOperator(new CustomAsyncFunctonTuple3_1(customCacheDataBaseConf),11111,2222, AsyncDataStream.OutputMode.ORDERED);
		DataStream dataStream = dataStreamSource1.transform("test",new TypeHint<Tuple3<String,String,Long>>(){}.getTypeInfo(),asyncWaitOperator);
		dataStream.addSink(new CustomPrintTuple3()).setParallelism(1);
	}

}
