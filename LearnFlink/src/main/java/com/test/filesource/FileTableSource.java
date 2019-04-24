package com.test.filesource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileTableSource implements
	StreamTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes,
	DefinedFieldMapping {
	private TableSchema schema;
	private DeserializationSchema<Row> deserializationS;
	private String rowTime;
	private String path;

	private FileTableSource(TableSchema schema, DeserializationSchema<Row> deserializationS, String rowTime, String path) {
		this.schema = schema;
		this.deserializationS = deserializationS;
		this.rowTime = rowTime;
		this.path = path;
	}

	@Nullable
	@Override
	public Map<String, String> getFieldMapping() {
		return null;
	}

	@Nullable
	@Override
	public String getProctimeAttribute() {
		return null;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor(rowTime, new ExistingField(rowTime),new BoundedOutOfOrderTimestamps(0L));
		return new ArrayList<RowtimeAttributeDescriptor>(){{
			add(0,rowtimeAttributeDescriptor);
		}};
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new FileSourceBase<Row>(deserializationS,path),"filesource",deserializationS.getProducedType());
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return deserializationS.getProducedType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public String explainSource() {
		return "";
	}

	public static FileTableSource.Builder builder() {
		return new FileTableSource.Builder();
	}

	public static class Builder{
		private TableSchema schema;
		private DeserializationSchema<Row> deserializationS;
		private String rowTime;
		private String path;

		public Builder setSchema(TableSchema schema) {
			this.schema = schema;
			return this;
		}

		public Builder setDeserializationS(DeserializationSchema<Row> deserializationS) {
			this.deserializationS = deserializationS;
			return this;
		}

		public Builder setRowTime(String rowTime) {
			this.rowTime = rowTime;
			return this;
		}

		public Builder setPath(String path) {
			this.path = path;
			return this;
		}

		protected Builder builder(){
			return this;
		}

		public FileTableSource build(){
			return new FileTableSource(schema,deserializationS,rowTime,path);
		}


	}
}
