package com.test.tablesource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;

public class CustomSource implements StreamTableSource<Row>,DefinedRowtimeAttributes{

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return null;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {


		return null;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return null;
	}

	@Override
	public TableSchema getTableSchema() {
		return null;
	}

	@Override
	public String explainSource() {
		return "";
	}
}
