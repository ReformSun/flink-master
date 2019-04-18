package com.test.filesource;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.test.util.URLUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileSourceTuple3 implements SourceFunction<Tuple3<String,Integer,Long>> {
	private String path;
	// 单位毫秒 调整数据发送速度
	private long interval = 0;
	public FileSourceTuple3() {
		path = URLUtil.baseUrl + "source.txt";
	}

	public FileSourceTuple3(long interval) {
		this.interval = interval;
		path = URLUtil.baseUrl + "source.txt";
	}

	public FileSourceTuple3(String path) {
		this.path = path;
	}
	@Override
	public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
		Path logFile = Paths.get(path);
		try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
			String line;
			while (( line = reader.readLine()) != null){
				List<String> list2 = Splitter.on(",").trimResults(CharMatcher.is('(').or(CharMatcher.is(')'))).splitToList(line);
				if (list2.size() == 3){
					Tuple3<String,Integer,Long> tuple3 = new Tuple3<>();
					tuple3.f0 = list2.get(0);
					tuple3.f1 = Integer.valueOf(list2.get(1));
					tuple3.f2 = Long.valueOf(list2.get(2));
					ctx.collect(tuple3);
				}
				if (interval > 0){
					Thread.sleep(interval);
				}

			}
		}

	}
	@Override
	public void cancel() {

	}
}
