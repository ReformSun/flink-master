package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.Properties;

public class TestMain {
    public static void main(String[] args) throws Exception {
//        配置环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
        CsvTableSource.Builder csvTableSourceBuilder = CsvTableSource
                .builder();


        CsvTableSource csvTableSource = csvTableSourceBuilder.path("E:\\Asunjihua\\idea\\flink-master\\LearnFlink\\src\\main\\resources\\testCsv.csv")//文件路径
                .field("name", Types.STRING)//第一列
                .field("age", Types.INT)//第二列
                .field("rex", Types.STRING)//第三列
//                .fieldDelimiter(",")//列分隔符，默认是逗号
//                .lineDelimiter("\n")//行分隔符，回车
                .ignoreFirstLine()//忽略第一行
                .build();//构建


        //将csv文件注册成表
        tableEnv.registerTableSource("testTable", csvTableSource);
//		testMethod2(tableEnv);
//		testMethod1(tableEnv);
//		testMethod3(tableEnv);
//		testMethod4(tableEnv);
		testMethod5(tableEnv);
        //执行
        sEnv.execute();

    }

	public static void testMethod5(StreamTableEnvironment tableEnv) {
		Table sqlResult = tableEnv.sql("select CONCAT(cast(age as VARCHAR),cast(rex as VARCHAR)) from testTable");
		sqlResult.writeToSink(new CsvTableSink("file:///E:\\Asunjihua\\idea\\flink-master\\LearnFlink\\src\\main\\resources\\aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
	}

	public static void testMethod4(StreamTableEnvironment tableEnv) {
		//查询
		Table sqlResult = tableEnv.sql("select COUNT(DISTINCT(age)) from testTable");
		sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
	}

	public static void testMethod3(StreamTableEnvironment tableEnv) {
		//查询
		Table sqlResult = tableEnv.sql("select * from testTable");
		Table sqlResult2 = sqlResult.select("age.cast(FLOAT)");
		sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
	}

	public static void testMethod1(StreamTableEnvironment tableEnv) {
		//查询
		Table sqlResult = tableEnv.sql("select * from testTable");
		//将数据写出去
//        sqlResult.printSchema();
		sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
	}


	public static void testMethod2(StreamTableEnvironment tableEnv) {
		Properties propertie = new Properties();
		propertie.setProperty("output-topic","ddddd");
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");
		String[] fieldNames = new String[3];
		TypeInformation[] typeInformations = {Types.STRING,Types.INT,Types.STRING};
		TypeInformation<Row> rowSchema = new RowTypeInfo(typeInformations);
		Table sqlResult = tableEnv.sql("select * from testTable");
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,Row.class);
		FlinkKafkaProducer010 flinkKafkaProducer010 = new FlinkKafkaProducer010<Row>(propertie.getProperty("output-topic"), new JsonRowSerializationSchema(rowSchema), propertie);
		stream.addSink(flinkKafkaProducer010);
	}
}
