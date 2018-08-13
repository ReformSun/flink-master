package com.test.customTableSource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.*;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class SocketTableSource implements
	StreamTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes,
	DefinedFieldMapping {
	@Override
	public Map<String, String> getFieldMapping() {
		return null;
	}

	@Override
	public String getProctimeAttribute() {
		return null;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return null;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return null;
	}

	@Override
	public String explainSource() {

		return "";
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return null;
	}

	@Override
	public TableSchema getTableSchema() {
		return null;
	}
}
