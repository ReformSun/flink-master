package com.test.learnWindows;

import com.test.flatMap_1.SunFunctionStates1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class TestStates {
    public static void main(String[] args) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setStateBackend(new RocksDBStateBackend("file:///Users/apple/Desktop/rockdata"));
        env.fromElements(Tuple2.of(1L,3L), Tuple2.of(1L,5L), Tuple2.of(1L,3L), Tuple2.of(1L,7L))
                .keyBy(0)
                .flatMap(new SunFunctionStates1())
                .print().setParallelism(1);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
