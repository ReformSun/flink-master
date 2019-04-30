package com.test.learnWindows;


import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.util.DataUtil;
import com.test.util.FileWriter;
import com.test.window.EventTimeTrigger;
import com.test.window.TumblingEventTimeWindows;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.KeyMap;

import static org.apache.flink.streaming.api.windowing.time.Time.*;

import java.io.IOException;
import java.util.*;

/**
 * 处理网络io的执行者 开始部分 可以从这类学 这是一个节点的开始
 * {@link org.apache.flink.streaming.runtime.io.StreamInputProcessor}
 * 窗口主执行者 在WindowOperator的open方法中开始HeapInternalTimerService任务并把自己传给HeapInternalTimerService
 * 任务，当定时器被触发时。执行WindowOperator的onEventTime方法,然后会调用自定义的或者EventTimeTrigger触发器
 * 通过触发器决定窗口是否被触发
 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}
 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.WindowContext}
 * // 管理定时任务的服务 任务中的advanceWatermark方法可触发注册进服务中的定时器
 * {@link org.apache.flink.streaming.api.operators.HeapInternalTimerService}
 * // 执行时间戳的执行者
 * {@link org.apache.flink.streaming.runtime.operators.TimestampsAndPunctuatedWatermarksOperator}
 *
 */
public class LearnTimeWindow {
	static Tuple3<String,Integer,Long> tuple3 = new Tuple3<>();
	static {
		tuple3.f0 = "a";
		tuple3.f1 = 1;
		tuple3.f2 = 1534472000000L;
	}

	public static void main(String[] args) {
//		testMethod2();
//		testMethod2_1();
		testMethod2_2();
//		testMethod4();
	}

	public static void testMethod1(){
		long start = 1534472000000L;
		long end = 1534472000000L;
		TimeWindow timeWindow = new TimeWindow(start,end);
	}

	/**
	 *
	 *
	 * 0 <= offset < size
	 * 翻滚窗口
	 * 比如我想计算每一分钟内数据的某个值的总和 但是想延迟50秒统计
	 * size 60000 单位毫秒
	 * offset 50000 单位毫秒
	 *
	 * 学习原理
	 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}
	 * WindowOperator类的processElement方法的第一步
	 * // 获取分配窗口
	 * final Collection<W> elementWindows = windowAssigner.assignWindows(
	 * element.getValue(), element.getTimestamp(), windowAssignerContext);
	 */
	public static void testMethod2(){
		TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(minutes(1),seconds(50));
		Collection<TimeWindow> collection = tumblingEventTimeWindows.assignWindows(tuple3,tuple3.f2,null);
		for (TimeWindow timeWindow:collection){
			System.out.println(timeWindow.toString());
			System.out.println(timeWindow.getEnd() - timeWindow.getStart());
		}
	}

	public static void testMethod2_1(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile("");
		Iterator<Tuple3<String,Integer,Long>> iterator = list.iterator();
		TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(minutes(1),seconds(0));
		while (iterator.hasNext()){
			Tuple3<String,Integer,Long> tuple = iterator.next();
			Collection<TimeWindow> collection = tumblingEventTimeWindows.assignWindows(tuple,tuple.f2,null);
			System.out.println(collection.toArray()[0]);
		}
	}

	public static void testMethod2_2(){
		List<Map<String,Object>> list = DataUtil.getList_MapFromFile(null);
		HashMap<Long,Integer> mapp = new HashMap<>();
		Iterator<Map<String,Object>> iterator = list.iterator();
		TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(minutes(1),seconds(0));
		while (iterator.hasNext()){
			Map<String,Object> map = iterator.next();
			Collection<TimeWindow> collection = tumblingEventTimeWindows.assignWindows(map,(Long) map.get("_sysTime"),null);
			TimeWindow timeWindow = (TimeWindow) collection.toArray()[0];
			if (mapp.containsKey(timeWindow.getStart())){
				mapp.compute(timeWindow.getStart(),(a,b)->{
					return b + (Integer) map.get("user_count");
				});
			}else {
				mapp.put(timeWindow.getStart(),(Integer) map.get("user_count"));
			}
			System.out.println(collection.toArray()[0]);
		}

		for (Map.Entry<Long,Integer> entry:mapp.entrySet()){
			try {
				FileWriter.writerFile(entry.toString(),"test.txt");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void testMethod3(){
		EventTimeTrigger eventTimeTrigger = EventTimeTrigger.create();
	}

	public static void testMethod4(){
		CustomAssignerTimesTampTyple3 customAssignerTimesTampTyple3 = new CustomAssignerTimesTampTyple3(2);
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		Iterator<Tuple3<String,Integer,Long>> iterator = list.iterator();
		while (iterator.hasNext()){
			Tuple3<String,Integer,Long> tuple = iterator.next();
			customAssignerTimesTampTyple3.extractTimestamp(tuple,0);
			System.out.println(customAssignerTimesTampTyple3.checkAndGetNextWatermark(tuple,0).toString());
		}
	}
	
	public static void testMethod5(){

	}
}
