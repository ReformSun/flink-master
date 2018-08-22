package com.test.learnTableapi;

import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 流上的Join分为两种，流和表的Join以及流和流的Join流上的Join实际都是窗口和窗口的JOin，或者窗口和表的Join，本质上都是表之间的Join，因为窗口就是一张表。
 */
public class TestMain11 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("k", Types.STRING).field("rtime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain11_1",tableSchema,"rtime");
		tableEnv.registerTableSource("kafkasource1", kafkaTableSource);


		TableSchemaBuilder tableSchemaBuilder2 = TableSchema.builder();
		TableSchema tableSchema2 = tableSchemaBuilder2.field("username", Types.STRING).field("sysTime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource2 = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain11_2",tableSchema2,"sysTime");
		tableEnv.registerTableSource("kafkasource2", kafkaTableSource2);
//		testMethod4(tableEnv);
//		testMethod1(tableEnv);
		testMethod2(tableEnv);
		sEnv.execute();
	}

	/**
	 * 双流join
	 * @param tableEnvironment
	 */
	public static void testMethod4(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT t1.k,t2.sysTime FROM kafkasource1 as t1 JOIN kafkasource2 as t2 ON t1.k = t2.username AND t1.rtime BETWEEN t2.sysTime AND t2.sysTime + INTERVAL '1' HOUR");
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod2(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table table = tableEnvironment.sqlQuery("select * from kafkasource2");
		Table sqlResult = tableEnvironment.scan("kafkasource1").join(table).where("k = username").select("k,username,sysTime");
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod1(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT * FROM( SELECT k FROM kafkasource1) UNION ALL (SELECT username FROM kafkasource2)");
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}

	/**
	 * 流和表的join
	 */
	public static void testMethod3(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT * FROM( SELECT k FROM kafkasource1) UNION ALL (SELECT username FROM kafkasource2)");
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}
}
