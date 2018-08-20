package com.test.customTableSource;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SocketTableSource implements
	StreamTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes,
	DefinedFieldMapping {

	/** The schema of the table. */
	private TableSchema schema;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private Optional<String> proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** Mapping for the fields of the table schema to fields of the physical returned type. */
	private Optional<Map<String, String>> fieldMapping;

	private  DeserializationSchema<Row> deserializationS;

	@Override
	public Map<String, String> getFieldMapping() {
		return null;
	}

	@Override
	public String getProctimeAttribute() {
		return null;
	}

	public SocketTableSource() {

		TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
		schema = tableSchemaBuilder.field("username", Types.STRING).field("countt", Types.INT).field("rtime", Types.SQL_TIMESTAMP).build();
		deserializationS = new JsonRowDeserializationSchema(getTableSchema().toRowType());
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {

		RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("rtime", new ExistingField("rtime"),new BoundedOutOfOrderTimestamps(0L));
		return new ArrayList<RowtimeAttributeDescriptor>(){{
			add(0,rowtimeAttributeDescriptor);
		}};
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new SocketSource(deserializationS),deserializationS.getProducedType());
	}

	@Override
	public String explainSource() {

		return "";
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return deserializationS.getProducedType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}
}
