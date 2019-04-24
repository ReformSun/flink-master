package com.test.sink;

import com.test.util.FileWriter;
import com.test.util.TimeUtil;
import com.test.util.URLUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CustomPrintTuple3 extends RichSinkFunction<Tuple3> {
	private String subjectContent;

	public CustomPrintTuple3(String subjectContent) {
		this.subjectContent = subjectContent;
	}

	public CustomPrintTuple3() {
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		if (subjectContent != null){
			FileWriter.writerFile(subjectContent + "==时间：" + TimeUtil.toDate(System.currentTimeMillis()),"test.txt");
		}
		super.open(parameters);
	}

	@Override
	public void invoke(Tuple3 value) throws Exception {
		FileWriter.writerFile(value.toString(),"test.txt");
	}
}
