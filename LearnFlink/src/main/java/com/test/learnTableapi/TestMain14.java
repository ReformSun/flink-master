package com.test.learnTableapi;

import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 测试环比上升 环比下降 同比上升 同比下降 如果job失败后的处理思路
 *
 * 问题1 如果数据比较时间过长 比如同比一天前的数据 如果遇到job失败重启后
 * 动态表中的数据怎么保证不丢失
 *
 * 问题2 如果在运行一段时间后 job任务需要增加并行度 重新启动后 任务状态怎样扩容
 */
public class TestMain14 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		sEnv.enableCheckpointing(6000);
		sEnv.setParallelism(1);
		FsStateBackend fsStateBackend = new FsStateBackend(new Path("file:///root/rockdata").toUri(),new Path("file:///root/savepoint").toUri());
		sEnv.setStateBackend(new RocksDBStateBackend(fsStateBackend));

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder.field("user_name", Types.STRING).field("user_count",Types.LONG).field("_sysTime", Types.SQL_TIMESTAMP).build();
		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("abcdef",tableSchema,"_sysTime");
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
		stream2.addSink(new CustomRowPrint("test.txt"));
	}
}
