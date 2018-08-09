package com.test.learnTableapi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.util.Collector;
import socket.SocketWindowWordCount;

public class TestMain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);

        DataStreamSource<String> streamSource = sEnv.socketTextStream("localhost", 9000, "\n");
        DataStream<SocketWindowWordCount.WordWithCount> windowCounts = streamSource
                .flatMap(new FlatMapFunction<String, SocketWindowWordCount.WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<SocketWindowWordCount.WordWithCount> out) {

                        for (String word : value.split("\\s")) {
                            out.collect(new SocketWindowWordCount.WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(5))
                .reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
                    @Override
                    public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount a, SocketWindowWordCount.WordWithCount b) {
                        return new SocketWindowWordCount.WordWithCount(a.word, a.count + b.count);
                    }
                });


        Table table = tableEnv.fromDataStream(windowCounts,"count,word");


        tableEnv.registerTable("testTable",table);

        System.out.println(tableEnv.explain(table));

        Table sqlResult = tableEnv.sql("select * from testTable");
        //将数据写出去
//        sqlResult.printSchema();
        sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
        //执行
        sEnv.execute();
    }
}

