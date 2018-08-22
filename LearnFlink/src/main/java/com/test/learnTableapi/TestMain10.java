package com.test.learnTableapi;

import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint1;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class TestMain10 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("k", Types.STRING).field("rtime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain10_1",tableSchema,"rtime");
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);
// 		testMethod2(tableEnv);
//		testMethod3(tableEnv);
		testMethod4(tableEnv);
		sEnv.execute();
	}

	public static void testMethod1(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT k,COUNT(k) as cnt FROM kafkasource GROUP BY k");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING,Types.LONG);
		DataStream<Tuple2<Boolean, Row>> stream = tableEnvironment.toRetractStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint1("test3.txt"));
	}

	public static void testMethod2(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT k,COUNT(k) as cnt,TUMBLE_START(rtime, INTERVAL '10' SECOND) as startTime FROM kafkasource GROUP BY k,TUMBLE(rtime, INTERVAL '10' SECOND)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING,Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}

	public static void testMethod3(StreamTableEnvironment tableEnvironment) {

		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT k,COUNT(k) as cnt,TUMBLE_START(rtime, INTERVAL '10' SECOND) as startTime FROM kafkasource GROUP BY k,TUMBLE(rtime, INTERVAL '10' SECOND)");
//		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING,Types.LONG,Types.SQL_TIMESTAMP);
//		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
			.setUsername("apm").setPassword("apm")
			.setQuery("insert into test10(k,cnt,rtime) values(?,?,?)")
			.setParameterTypes(STRING_TYPE_INFO,LONG_TYPE_INFO,SqlTimeTypeInfo.TIMESTAMP)
			.setBatchSize(1)
			.build();

		sqlResult.writeToSink(sink);

	}

	public static void testMethod4(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT k,COUNT(k) as cnt,TUMBLE_START(rtime, INTERVAL '10' SECOND) as startTime FROM kafkasource GROUP BY k,TUMBLE(rtime, INTERVAL '10' SECOND)");

		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING,Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);
		Table table = tableEnvironment.fromDataStream(stream,"k,b,c.rowtime");
		tableEnvironment.registerTable("table1",table);
		tableEnvironment.registerTable("table2",sqlResult);

//		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT * FROM table2 as t2 INNER JOIN table1 as t1 ON t1.c = t2.startTime - INTERVAL '30' SECOND AND t1.c BETWEEN t2.startTime - INTERVAL '30' SECOND AND t2.startTime");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT t1.k,t2.cnt FROM table2 as t2 INNER JOIN table1 as t1 ON t1.k = t2.k AND t2.startTime BETWEEN t1.c AND t1.c + INTERVAL '30' SECOND");
		DataStream<Row> stream2 = tableEnvironment.toAppendStream(sqlResult2, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
		stream2.addSink(new CustomRowPrint("test1.txt"));

	}
}
