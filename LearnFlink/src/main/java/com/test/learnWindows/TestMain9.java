package com.test.learnWindows;

import com.test.customAssignTAndW.CustomAssignerTimesTampTyple3;
import com.test.sink.CustomPrintTuple3;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;

/**
 * union
 */
public class TestMain9 extends AbstractTestMain1{
	public static void main(String[] args) {
		try{
			testMethod1();
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testMethod1() {

		DataStream<Tuple3<String,Integer,Long>> dataStreamSource1 = env.fromCollection(getTestdata()).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);
		DataStream<Tuple3<String,Integer,Long>> dataStreamSource2 = env.fromCollection(getTestdata()).assignTimestampsAndWatermarks(new CustomAssignerTimesTampTyple3()).setParallelism(1);

		DataStream dataStream = dataStreamSource1.union(dataStreamSource2);

		dataStream.addSink(new CustomPrintTuple3());

	}
}
