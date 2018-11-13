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

import java.util.ArrayList;
import java.util.List;

public class TestMain12 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("user_name", Types.STRING).field("user_count",Types.LONG).field("_sysTime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain11_3",tableSchema,"_sysTime");
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);
//		testMethod1(tableEnv);
//		testMethod3(tableEnv);
//		testMethod4(tableEnv);
//		testMethod5(tableEnv);
//		testMethod6(tableEnv);
//		testMethod7(tableEnv);
		testMethod8(tableEnv);
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

	public static void testMethod8(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT SUM(user_count) as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		Table table = tableEnvironment.fromDataStream(stream,"value1,start_time");
		tableEnvironment.registerTable("tableName1",table);
		tableEnvironment.registerTable("tableName2",sqlResult);
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT cast(t2.value1-t1.value1 as FLOAT) / t1.value1 * 100 <=20 as a748658 FROM tableName2 as t2 JOIN tableName1 as t1 ON t1.start_time = t2.start_time-INTERVAL '5' MINUTE");
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

	public static void testMethod3(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		List<Table> tableList = new ArrayList<>();
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 2 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 3 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table sqlResult3 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 4 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table sqlResult4 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 5 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");

		tableList.add(sqlResult1);
		tableList.add(sqlResult2);
		tableList.add(sqlResult3);
		tableList.add(sqlResult4);

		Table tableRe = null;
		for (Table table:tableList){
			if (tableRe == null){
				tableRe = table;
			}else {
				tableRe = tableRe.as("a,value1").join(table.as("b,value2")).where("value1 = value2").select("a.OR(b),value1");
			}
		}

		DataStream<Row> stream2 = tableEnvironment.toAppendStream(tableRe, Row.class, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod4(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 2 as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 3 as value1,TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table tableRe = sqlResult1.as("a,time1").join(sqlResult2.as("b,time2")).select("a.OR(b),time1,time2");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP);
		DataStream<Row> stream2 = tableEnvironment.toAppendStream(tableRe, rowTypeInfo, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod5(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		// TIME ATTRIBUTE(ROWTIME)
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 2 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(user_count) > 3 as value1,cast(TUMBLE_START(_sysTime, INTERVAL '1' MINUTE) as VARCHAR) FROM kafkasource WHERE user_name = '小张' GROUP BY TUMBLE(_sysTime, INTERVAL '1' MINUTE)");
		Table tableRe = sqlResult1.as("a,time1").join(sqlResult2.as("b,time2")).where("time1 = time2").select("a.OR(b),time1,time2");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.BOOLEAN,Types.STRING,Types.STRING);
		DataStream<Row> stream2 = tableEnvironment.toAppendStream(tableRe, rowTypeInfo, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod6(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		List<Table> tableList = new ArrayList<>();
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,cast(HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE) as VARCHAR) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,cast(HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE) as VARCHAR) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE)");
		Table sqlResult3 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,cast(HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE) as VARCHAR) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE)");
		Table sqlResult4 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,cast(HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as VARCHAR) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)");


		Table sqlR1 = sqlResult1.select("value1 > 1,start_time");
		Table sqlR2 = sqlResult1.select("value1 > 2,start_time");
		Table sqlR3 = sqlResult1.select("value1 > 3,start_time");
		Table sqlR4 = sqlResult1.select("value1 > 4,start_time");

		tableList.add(sqlR1);
		tableList.add(sqlR2);
		tableList.add(sqlR3);
		tableList.add(sqlR4);

		Table tableRe = null;
		for (Table table:tableList){
			if (tableRe == null){
				tableRe = table;
			}else {
				tableRe = tableRe.as("a,value1").join(table.as("b,value2")).where("value1 = value2").select("a.OR(b),value1");
			}
		}

		DataStream<Row> stream2 = tableEnvironment.toAppendStream(tableRe, Row.class, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod7(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		List<Table> tableList = new ArrayList<>();
		Table sqlResult0 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)");
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE)");
//		Table sqlResult3 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE)");
//		Table sqlResult4 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)");


		Table sqlR0 = sqlResult1.select("value1 > 1,start_time.cast(STRING)");
		Table sqlR1 = sqlResult1.select("value1 > 1,start_time.cast(STRING)");
		Table sqlR2 = sqlResult1.select("value1 > 2,start_time.cast(STRING)");
//		Table sqlR3 = sqlResult1.select("value1 > 3,start_time.cast(STRING)");
//		Table sqlR4 = sqlResult1.select("value1 > 4,start_time.cast(STRING)");


		tableList.add(sqlR0);
		tableList.add(sqlR1);
		tableList.add(sqlR2);
//		tableList.add(sqlR3);
//		tableList.add(sqlR4);

		Table tableRe = null;
		for (Table table:tableList){
			if (tableRe == null){
				tableRe = table;
			}else {
				tableRe = tableRe.as("a,value1").join(table.as("b,value2")).where("value1 = value2").select("a.OR(b),value1");
			}
		}

		DataStream<Row> stream2 = tableEnvironment.toAppendStream(tableRe, Row.class, qConfig);
		stream2.addSink(new CustomRowPrint("test3.txt"));
	}
}
