package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestMain3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Map<String, TypeInformation> columnMap = new HashMap<>();
        columnMap.put("_sysTime", Types.SQL_TIMESTAMP);
        columnMap.put("sensorId", Types.LONG);
        columnMap.put("ptime",Types.SQL_TIMESTAMP);
        Properties properties = new Properties();
        properties.setProperty("input-topic","monitorBlocklyQueueKey5");
        properties.setProperty("bootstrap.servers","172.31.24.30:9092");
        properties.setProperty("group.id","serverCollector");


        Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(properties.getProperty("input-topic"));

        jsonTableSourceBuilder.withKafkaProperties(properties);
        // set Table schema
        TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();
        for (Map.Entry<String, TypeInformation> entry : columnMap.entrySet()) {
            tableSchemaBuilder.field(entry.getKey(), entry.getValue());
        }

        jsonTableSourceBuilder.withSchema(tableSchemaBuilder.build());
//        jsonTableSourceBuilder.withProctimeAttribute("ptime");
        //values of "_sysTime" are at most out-of-order by 30 seconds
        jsonTableSourceBuilder.withRowtimeAttribute("_sysTime", new ExistingField("_sysTime"),new BoundedOutOfOrderTimestamps(30000L));

        KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();

        tableEnv.registerTableSource("kafkasource", kafkaTableSource);


        Table sqlResult = tableEnv.sql("select * from kafkasource");
        //将数据写出去
//        sqlResult.printSchema();
        sqlResult.writeToSink(new CsvTableSink("file:///Users/apple/Documents/AgentJava/intellProject/LearnFlink/src/main/resources/aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE));
        //执行
        sEnv.execute();
    }
}
