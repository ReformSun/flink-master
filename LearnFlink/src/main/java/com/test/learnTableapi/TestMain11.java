package com.test.learnTableapi;

import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
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
		testMethod4_1(tableEnv);
//		testMethod1(tableEnv);
//		testMethod2(tableEnv);
		sEnv.execute();
	}

	/**
	 * 双流join 无限流和带窗口
	 * @param tableEnvironment
	 */
	public static void testMethod4(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT t1.k,t2.sysTime FROM kafkasource1 as t1 JOIN kafkasource2 as t2 ON t1.k = t2.username AND t1.rtime BETWEEN t2.sysTime AND t2.sysTime + INTERVAL '1' HOUR");
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, Row.class, qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));
	}

	/**
	 * 双流jion中 如果通过时间条件jion
	 * SELECT t1.k,t2.sysTime FROM kafkasource1 as t1  JOIN kafkasource2 as t2 ON t1.rtime = t2.sysTime
	 * 上面的sql 语义 计算jion两个流中 时间相同的两条数据 语义是有界的 因为时间是唯一的
	 * 如果是
	 * SELECT t1.k,t2.sysTime FROM kafkasource1 as t1 JOIN kafkasource2 as t2 ON t1.k = t2.username
	 * 这条语句就不可以 上面表达的语义为 jion两个流中姓名相同的两条数据 姓名不是唯一的 有很多相同名字的字段 所以语义不是有界的
	 * 如果可以运行 就必须添加
	 * SELECT t1.k,t2.sysTime FROM kafkasource1 as t1 JOIN kafkasource2 as t2 ON t1.k = t2.username AND t1.rtime BETWEEN t2.sysTime AND t2.sysTime + INTERVAL '1' HOUR
	 * 上面的sql是可以的
	 * 语义为 jion两个流中姓名相同并且动态表ti的时间在动态表时间一个小时内的两条数据 为有界的数据
	 *
	 * @param tableEnvironment
	 */
	public static void testMethod4_1(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT t1.k,t2.sysTime FROM kafkasource1 as t1  JOIN kafkasource2 as t2 ON t1.rtime = t2.sysTime");
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

	public static void testMethod5(StreamTableEnvironment tableEnvironment,StreamExecutionEnvironment sEnv) {


		tableEnvironment.from(
			     new Kafka()
			       .version("0.11")
			       .topic("clicks")
			       .property("zookeeper.connect", "localhost")
			      .property("group.id", "click-group")
			       .startFromEarliest())
       .withFormat(
			     new Json()
			       .jsonSchema("{...}")
			      .failOnMissingField(false))
       .withSchema(
			     new Schema()
			       .field("user-name", "VARCHAR").from("u_name")
			       .field("count", "DECIMAL")
			       .field("proc-time", "TIMESTAMP").proctime())
      .toTable();

		StreamQueryConfig qConfig = new StreamQueryConfig();
		Table sqlResult = tableEnvironment.sqlQuery("SELECT k,COUNT(k) as cnt,TUMBLE_START(rtime, INTERVAL '10' SECOND) as startTime FROM kafkasource GROUP BY k,TUMBLE(rtime, INTERVAL '10' SECOND)");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING,Types.LONG,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnvironment.toAppendStream(sqlResult, rowTypeInfo, qConfig);

		stream.addSink(new CustomRowPrint("test3.txt"));

	}

	public static DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv){
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("org.postgresql.Driver")
			.setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
			.setUsername("apm")
			.setPassword("apm")
			.setQuery("select * from figure")
			.setRowTypeInfo(rowTypeInfo)
			.setFetchSize(3)
			.finish();

		return sEnv.createInput(jdbcInputFormat);
	}
}
