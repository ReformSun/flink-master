package com.test.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.types.Row;

public class CustomBatchSource extends AbstractTableInputFormat<Row> implements ResultTypeQueryable<Row> {
	@Override
	public TypeInformation<Row> getProducedType() {
		return null;
	}
}
