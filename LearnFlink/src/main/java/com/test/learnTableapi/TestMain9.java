package com.test.learnTableapi;

import com.test.defineFunction.Test;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestMain9 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("a", Types.STRING).field("d",Types.INT).field("b", Types.INT).field("rtime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJson2",tableSchema,"rtime");
		StreamQueryConfig qConfig = new StreamQueryConfig();

		tableEnv.registerTableSource("kafkasource", kafkaTableSource);
		Table sqlResult = tableEnv.sqlQuery("SELECT SUM(b) as bb,TUMBLE_START(rtime, INTERVAL '10' SECOND) as ttime FROM kafkasource GROUP BY TUMBLE(rtime, INTERVAL'10' SECOND),a");

		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.INT,Types.SQL_TIMESTAMP);
		DataStream<Row> stream = tableEnv.toAppendStream(sqlResult,rowTypeInfo,qConfig);
		stream.addSink(new CustomRowPrint("test3.txt"));


		Table table = tableEnv.fromDataStream(stream,"b,c");
		tableEnv.registerTable("table1",table);

		Table table1 = tableEnv.sqlQuery("select * from table1 where b > 0");
		DataStream<Row> stream2 = tableEnv.toAppendStream(table1,Row.class,qConfig);
		stream2.addSink(new CustomRowPrint("test.txt"));


//		tableEnv.registerTable("table2",sqlResult);

//		Table table1 = tableEnv.sqlQuery("select * from table1 INNER JOIN table2 ON table1.b = table2.bb");



		sEnv.execute();
	}
}
