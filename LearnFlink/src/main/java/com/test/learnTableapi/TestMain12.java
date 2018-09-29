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

public class TestMain12 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("user_name", Types.STRING).field("user_count",Types.LONG).field("_sysTime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain11_1",tableSchema,"_sysTime");
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);
		testMethod1(tableEnv);
		sEnv.execute();
	}

	public static void testMethod1(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT AVG(user_count) as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		Table table = tableEnvironment.fromDataStream(stream,"value1,start_time");
		tableEnvironment.registerTable("tableName1",table);
		tableEnvironment.registerTable("tableName2",sqlResult);
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT cast(t2.value1-t1.value1 as FLOAT) / t1.value1 * 100  as a748658 FROM tableName2 as t2 JOIN tableName1 as t1 ON t1.start_time = t2.start_time-INTERVAL '1' MINUTE");
		DataStream<Row> stream2 = tableEnvironment.toAppendStream(sqlResult2, Row.class, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}

	// 测试报警
	public static void testMethod2(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 2 as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		Table table = tableEnvironment.fromDataStream(stream,"value1,start_time");
		tableEnvironment.registerTable("tableName1",table);
		tableEnvironment.registerTable("tableName2",sqlResult);
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT cast(t2.value1-t1.value1 as FLOAT) / t1.value1 * 100  as a748658 FROM tableName2 as t2 JOIN tableName1 as t1 ON t1.start_time = t2.start_time-INTERVAL '1' MINUTE");
		DataStream<Row> stream2 = tableEnvironment.toAppendStream(sqlResult2, Row.class, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}
}
