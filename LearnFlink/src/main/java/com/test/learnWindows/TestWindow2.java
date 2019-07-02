package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.sink.CustomPrintTuple3;
import com.test.util.DataUtil;
import com.test.util.URLUtil;
import com.test.window.TumblingEventTimeWindows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.SumFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class TestWindow2 extends AbstractTestMain11{
	public static void main(String[] args) {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		try{
			common();
		}catch (Exception e){
			e.printStackTrace();
		}
		try {
			env.execute("TestWindow1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void common(){
		List<Tuple3<String,Integer,Long>> list = DataUtil.getListFromFile(null);
		System.out.println(list.size() + " asdfg");
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(list).setParallelism(1)
			.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3(2))
			.setParallelism(1);
		KeyedStream<Tuple3<String,Integer,Long>,String> keyedStream = dataStreamSource1
			.keyBy(new KeySelector<Tuple3<String,Integer,Long>, String>() {
				@Override
				public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
//					FileWriter.writerFile(value,"test1.txt");
					return value.getField(0);
				}
			});
		WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream = keyedStream
			.window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(0)));
//		testMethod1(windowedStream);
		testMethod2(windowedStream);
	}

	public static void testMethod1(WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream){
		TupleTypeInfo<Tuple3<String,Integer,Long>> tuple3TupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.INT(),Types.LONG());
		DataStream<Tuple3<String, Integer, Long>> dataStream = windowedStream.reduce(new SumAggregator(1,tuple3TupleTypeInfo,null));
		sink(dataStream);
	}

	public static void testMethod2(WindowedStream<Tuple3<String,Integer,Long>,String,TimeWindow> windowedStream){
		TupleTypeInfo<Tuple3<String,Integer,Long>> tuple3TupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.INT(),Types.LONG());
		DataStream<Tuple3<String, Integer, Long>> dataStream = windowedStream.reduce(new SumAggregator(1,tuple3TupleTypeInfo,null),new CustomWindowFunction());
		sink(dataStream);
	}

	private static void sink(DataStream<Tuple3<String, Integer, Long>> dataStream){
		dataStream.addSink(new SinkFunction<Tuple3<String, Integer, Long>>() {
			@Override
			public void invoke(Tuple3<String, Integer, Long> value) throws Exception {
				java.nio.file.Path logFile = Paths.get(URLUtil.baseUrl + "test.txt");
				try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
					writer.newLine();
					writer.write(value.toString());
				}
			}
		});
	}



	static class CustomWindowFunction implements WindowFunction<Tuple3<String,Integer,Long>,Tuple3<String,Integer,Long>,String,TimeWindow>{
		@Override
		public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
			for (Tuple3<String, Integer, Long> in: input) {
				out.collect(in);
			}
		}
	}
	static class SumAggregator extends AggregationFunction<Tuple3<String, Integer, Long>> {

		private static final long serialVersionUID = 1L;

		private final FieldAccessor<Tuple3<String, Integer, Long>, Object> fieldAccessor;
		private final SumFunction adder;
		private final TypeSerializer<Tuple3<String, Integer, Long>> serializer;
		private final boolean isTuple;

		public SumAggregator(int pos, TypeInformation<Tuple3<String, Integer, Long>> typeInfo, ExecutionConfig config) {
			fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
			adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
			if (typeInfo instanceof TupleTypeInfo) {
				isTuple = true;
				serializer = null;
			} else {
				isTuple = false;
				this.serializer = typeInfo.createSerializer(config);
			}
		}

		public SumAggregator(String field, TypeInformation<Tuple3<String, Integer, Long>> typeInfo, ExecutionConfig config) {
			fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, field, config);
			adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
			if (typeInfo instanceof TupleTypeInfo) {
				isTuple = true;
				serializer = null;
			} else {
				isTuple = false;
				this.serializer = typeInfo.createSerializer(config);
			}
		}

		@Override
		public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
			if (isTuple) {
				Tuple3<String, Integer, Long> result = value1.copy();
				return fieldAccessor.set(result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
			} else {
				Tuple3<String, Integer, Long> result = serializer.copy(value1);
				return fieldAccessor.set(result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
			}
		}
	}

}
