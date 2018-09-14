package com.test.util;

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
        Path logFile = Paths.get(".\\LearnFlink\\src\\main\\resources\\" + fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)){
            writer.newLine();
            writer.write(s);
        }

    }
}
