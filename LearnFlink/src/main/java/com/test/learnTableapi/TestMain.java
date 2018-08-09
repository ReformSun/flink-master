package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class TestMain {
    public static void main(String[] args) throws Exception {
//        配置环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
        CsvTableSource.Builder csvTableSourceBuilder = CsvTableSource
                .builder();


        CsvTableSource csvTableSource = csvTableSourceBuilder.path("/Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/testCsv.csv")//文件路径
                .field("name", Types.STRING)//第一列
                .field("age", Types.INT)//第二列
                .field("rex", Types.STRING)//第三列
//                .fieldDelimiter(",")//列分隔符，默认是逗号
//                .lineDelimiter("\n")//行分隔符，回车
                .ignoreFirstLine()//忽略第一行
                .build();//构建


        //将csv文件注册成表
        tableEnv.registerTableSource("testTable", csvTableSource);
        //查询
        Table sqlResult = tableEnv.sql("select * from testTable");
        //将数据写出去
//        sqlResult.printSchema();
        sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
        //执行
        sEnv.execute();

    }
}
