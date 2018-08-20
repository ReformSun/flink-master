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
	public Timestamp eval(Timestamp time,Integer b){
		long ti = time.getTime();
		long c = ti - b;

		return new Timestamp(c);
	}

}
