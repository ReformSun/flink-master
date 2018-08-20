package com.test.defineFunction;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;

public class Test2 extends AggregateFunction<Float,TestModel2>{

	private static int i = 0;

	@Override
	public TestModel2 createAccumulator() {
		return new TestModel2();
	}

	@Override
	public Float getValue(TestModel2 acc) {
		System.out.println(i);
		return Float.valueOf(acc.lastValue - acc.firstValue);
	}


	public void accumulate(TestModel2 acc,Integer sum){

		try {
			writerFile(i + " 数字：" + sum );
		} catch (IOException e) {
			e.printStackTrace();
		}
		i ++;
	}

	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test3.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}


//	public void merge(TestModel2 acc, Timestamp timestamp,Integer sum){
//
//		System.out.println(i + "时间:" + timestamp + " 数字：" + sum );
//		i ++;
//	}



}
