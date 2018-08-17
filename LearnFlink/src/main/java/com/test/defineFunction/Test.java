package com.test.defineFunction;

import org.apache.flink.table.functions.ScalarFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;

public class Test extends ScalarFunction{
	public int eval(Timestamp time,Integer b){
		try {
			writerFile(b + "  dd   " + time);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 12;
	}

	public  void writerFile(String s) throws IOException {
		Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\test3.txt");
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(s);
		}
	}
}
