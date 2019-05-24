package com.test.learnTableapi;

import com.test.filesource.FileSourceBase;
import com.test.filesource.FileTableSource;
import com.test.util.URLUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.types.Row;

public class FileUtil {
	public static FileTableSource getFileTableSource(long time, WatermarkStrategy watermarkStrategy){
		FileTableSource.Builder builder = FileTableSource.builder();

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder
			.field("user_name", Types.STRING)
			.field("user_count",Types.LONG)
			.field("_sysTime", Types.SQL_TIMESTAMP)
			.build();

		DeserializationSchema<Row> deserializationS = new JsonRowDeserializationSchema(tableSchema.toRowType());

		return builder.setSchema(tableSchema)
			.setDeserializationS(deserializationS)
			.setPath(URLUtil.baseUrl + "dataTestTableFile.txt")
			.setRowTime("_sysTime")
			.setInterval(time)
			.setWatermarkStrategy(watermarkStrategy)
			.build();
	}

	public static FileTableSource getFileTableSource(){
		return getFileTableSource(10000,null);
	}

	public static FileTableSource getFileTableSource(long time){
		return getFileTableSource(time,null);
	}

	public static FileSourceBase<Row> getFileSourceBase(){
		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		TableSchema tableSchema = tableSchemaBuilder
			.field("user_name", Types.STRING)
			.field("user_count",Types.LONG)
			.field("_sysTime", Types.LONG)
			.build();

		DeserializationSchema<Row> deserializationS = new JsonRowDeserializationSchema(tableSchema.toRowType());
		FileSourceBase<Row> fileSourceBase = new FileSourceBase(deserializationS,URLUtil.baseUrl + "dataTestTableFile.txt",1000);
		return fileSourceBase;
	}
}
