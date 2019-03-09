package com.test.sdn;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple8;
import com.test.sink.CustomPrint;
import com.test.sink.CustomPrintTuple8;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;

public class TestMain1 {
	private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	public static void main(String[] args) {
		testMethod1();
		try {
			env.execute("测试");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1(){
		Path path = null;
		try {
			path = new Path(new URI("file:///Users/apple/Documents/AgentJava/flink-master/LearnFlink/src/main/resources/sdn.csv"));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
//		CsvReader csvReader = new CsvReader(path,env);
//		DataSource<Tuple8<String,String,String,String,Long,String,Boolean,Long>> dataSource = csvReader.ignoreFirstLine().types(String.class,String.class,String.class,String.class,
//			Long
//				.class,
//			String
//				.class,
//			Boolean
//				.class,
//			Long
//			.class);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TupleTypeInfo<Tuple8<String,String,String,String,Long,String,Boolean,Long>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(String.class,String.class,String.class,String.class,
			Long
				.class,
			String
				.class,
			Boolean
				.class,
			Long
				.class);

		TupleCsvInputFormat tupleCsvInputFormat = new TupleCsvInputFormat(path,types);
		tupleCsvInputFormat.setSkipFirstLineAsHeader(true);
		DataStreamSource<Tuple8<String,String,String,String,Long,String,Boolean,Long>> dataStreamSource = env.createInput(tupleCsvInputFormat,types);

		dataStreamSource.assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple8(4))
			.keyBy(new CustomKeySelector())
			.window(TumblingEventTimeWindows.of(Time.seconds(60)))
			.apply(new CustomWindowFunction()).addSink(new CustomPrint("test.txt"));
	}
}
