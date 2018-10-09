package com.test.learnTableapi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.test.sink.CustomRowPrint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

public class AlarmMain {
	public static void main(String[] args) throws Exception {

		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://10.4.247.20:5432/gpexmp";
		String username = "apm";
		String password = "apm";
		Connection connection= DriverManager.getConnection(url,username,password);
		Statement stmt = connection.createStatement() ;
		String sqlStr2 = "SELECT dataset_column,time_column FROM \"monitor_dataset\" WHERE dataset_id='" + 107 + "'" ;
		ResultSet rs = stmt.executeQuery(sqlStr2);
		rs.next();
		String dataset_column = rs.getString("dataset_column");
		Map<String, TypeInformation> kafakacolumnMap = processingQueryResults(dataset_column);
		TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();
		for (Map.Entry<String, TypeInformation> entry : kafakacolumnMap.entrySet()) {
			tableSchemaBuilder.field(entry.getKey(), entry.getValue());
		}

//		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);
//
//		sEnv.getConfig().disableSysoutLogging();
//		sEnv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//		sEnv.enableCheckpointing(5000);
//		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//		TableSchema tableSchema = tableSchemaBuilder.build();
//		KafkaTableSource kafkaTableSource = KafkaUtil.getKafkaTableSource("monitorBlocklyQueueKeyJsonTestMain11_3",tableSchema,rs.getString("time_column"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamQueryConfig qConfig = tableEnv.queryConfig();

		Properties propertie = new Properties();
		propertie.setProperty("input-topic","monitorBlocklyQueueKeyJsonTestMain11_3");
		propertie.setProperty("bootstrap.servers", "172.31.24.30:9092");
		propertie.setProperty("group.id", "serverCollector");

		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
//		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(propertie.getProperty("input-topic"));
		jsonTableSourceBuilder.withKafkaProperties(propertie);

		jsonTableSourceBuilder.withSchema(tableSchemaBuilder.build());
		jsonTableSourceBuilder.withRowtimeAttribute(rs.getString("time_column"), new ExistingField(rs.getString("time_column")),new BoundedOutOfOrderTimestamps(30000L));
		KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();
		tableEnv.registerTableSource("kafkasource", kafkaTableSource);

		testMethod1(tableEnv);
		env.execute();
	}






	public static void testMethod1(StreamTableEnvironment tableEnvironment) {
		StreamQueryConfig qConfig = tableEnvironment.queryConfig();
		List<Table> tableList = new ArrayList<>();
		Table sqlResult0 = tableEnvironment.sqlQuery("SELECT AVG(SUM_sales_index) as value1,HOP_START(datatime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE) as start_time FROM kafkasource GROUP BY HOP(datatime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)");
		Table sqlResult1 = tableEnvironment.sqlQuery("SELECT AVG(MAX_sales_index) as value1,HOP_START(datatime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) as start_time FROM kafkasource GROUP BY HOP(datatime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)");
		Table sqlResult2 = tableEnvironment.sqlQuery("SELECT AVG(SUM_sales_index) as value1,HOP_START(datatime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE) as start_time FROM kafkasource GROUP BY HOP(datatime, INTERVAL '1' MINUTE, INTERVAL '3' MINUTE)");
//		Table sqlResult3 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '4' MINUTE)");
//		Table sqlResult4 = tableEnvironment.sqlQuery("SELECT AVG(user_count)  as value1,HOP_START(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as start_time FROM kafkasource WHERE user_name = '小张' GROUP BY HOP(_sysTime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)");


		Table sqlR0 = sqlResult0.select("value1 >= 200 as a616275,start_time.cast(STRING) as start_time");
		Table sqlR1 = sqlResult1.select("value1 >= 1 as a439426,start_time.cast(STRING) as start_time");
		Table sqlR2 = sqlResult2.select("value1 >= 300 as a042764,start_time.cast(STRING) as start_time");
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





	private static Map<String, TypeInformation> processingQueryResults(String jString) throws SQLException {
		Map<String, TypeInformation> map = new HashMap();
		Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();
		Type type = new TypeToken<Map<String, Object>>(){}.getType();
		Map<String, Object> innerjsonMap = gson.fromJson(jString, type);
		for (String keyString : innerjsonMap.keySet()) {
			String value=innerjsonMap.get(keyString).toString();
			map.put(keyString,getTypeInformation(value));
		}
		return map;
	}

	private static TypeInformation getTypeInformation(String blockType){
		switch (blockType){
			case "string" : return org.apache.flink.api.common.typeinfo.Types.STRING;
			case "long": return org.apache.flink.api.common.typeinfo.Types.LONG;
			case "date": return org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
			case "double" : return org.apache.flink.api.common.typeinfo.Types.DOUBLE;
			default:return Types.STRING;
		}
	}
}
