package com.test.util;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileWriter {
    public static void main(String[] args) {
        try {

            writerFile("cffhh","test.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void writerFile(String s,String fileName) throws IOException {
        Path logFile = Paths.get(URLUtil.baseUrl + fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
            writer.newLine();
            writer.write(s);
        }
    }

	public static void writerFile(Tuple3 tuple3, String fileName) throws IOException {
		Path logFile = Paths.get(URLUtil.baseUrl + fileName);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
			writer.newLine();
			writer.write(tuple3.toString());
		}

	}
}
