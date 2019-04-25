package com.test.learnTableapi;

import com.test.filesource.FileTableSource;
import com.test.util.URLUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.types.Row;

public class FileUtil {
	public static FileTableSource getFileTableSource(){
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
			.setInterval(10000)
			.build();
	}
}
