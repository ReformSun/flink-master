package com.test.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileReader {

    public static void main(String[] args) {
        Iterator<String> iterator = null;
        try {
            iterator = readFile("").iterator();
            while (iterator.hasNext()){
                System.out.println(iterator.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static List<String> readFile(String path) throws IOException {
        Path logFile = Paths.get(path);
        try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)){
            List<String> list = new ArrayList<>();
            String line;
            while (( line = reader.readLine()) != null){
                list.add(line);
            }
            return list;
        }

    }

}
