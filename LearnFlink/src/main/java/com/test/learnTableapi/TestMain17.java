package com.test.learnTableapi;

import com.test.filesource.FileSource;
import com.test.filesource.FileTableSource;
import com.test.sink.CustomRowPrint;
import com.test.sink.CustomRowPrint_Sum;
import com.test.util.StreamExecutionEnvUtil;
import com.test.util.URLUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Arrays;

public class TestMain17 {
	public static void main(String[] args) {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvUtil.getStreamExecutionEnvironment();
		sEnv.setParallelism(1);
		TableConfig tableConfig = new TableConfig();
		tableConfig.setIsEnableWindowOutputTag(true,"");
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv,tableConfig);
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TableSchemaBuilder tableSchemaBuilder=TableSchema.builder();
		tableSchemaBuilder.field("user_count", Types.INT())
			.field("user_name",Types.STRING())
			.field("_sysTime",Types.LONG());
		JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema(tableSchemaBuilder.build().toRowType());
		FileSource<Row> fileSource = new FileSource<Row>(URLUtil.baseUrl + "dataTestTableFile.txt",jsonRowDeserializationSchema);
		DataStreamSource<Row> dataStreamSource = sEnv.addSource(fileSource,tableSchemaBuilder.build().toRowType());
		SplitStream<Row> splitStream = dataStreamSource.split(new OutputSelector<Row>(){
			@Override
			public Iterable<String> select(Row value) {
				String name = (String) value.getField(1);
				if (name.equals("小张")){
					return Arrays.asList("小张");
				}else {
					return Arrays.asList("other");
				}
			}
		});

		String fields = "user_count,user_name,_sysTime.rowtime";

		DataStream<Row> dataStream = splitStream.select("小张").assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
			BoundedOutOfOrderTimestamps boundedOutOfOrderTimestamps = new BoundedOutOfOrderTimestamps(0);
			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return boundedOutOfOrderTimestamps.getWatermark();
			}

			@Override
			public long extractTimestamp(Row element, long previousElementTimestamp) {
				Long time = (Long) element.getField(2);
				boundedOutOfOrderTimestamps.nextTimestamp(time);
				return time;

			}
		});
		tableEnv.registerDataStream("test",dataStream,fields);

		Table sqlResult = tableEnv.scan("test")
			.window(Tumble.over("1.minutes").on("_sysTime").as("w"))
			.groupBy("w")
			.select("SUM(user_count) as value1,w.start");
		RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.INT(),Types.SQL_TIMESTAMP());
		SingleOutputStreamOperator<Row> stream = (SingleOutputStreamOperator)tableEnv.toAppendStream(sqlResult, rowTypeInfo);
		stream.addSink(new CustomRowPrint("test.txt"));

		try {
			sEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
