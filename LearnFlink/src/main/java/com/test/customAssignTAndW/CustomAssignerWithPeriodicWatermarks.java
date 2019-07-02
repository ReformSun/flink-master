package com.test.customAssignTAndW;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Date;

public class CustomAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Row> {
    private BoundedOutOfOrderTimestamps boundedOutOfOrderTimestamps = new BoundedOutOfOrderTimestamps(0);
    private int index;

    public CustomAssignerWithPeriodicWatermarks(int index) {
        this.index = index;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return boundedOutOfOrderTimestamps.getWatermark();
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
		Object object = element.getField(index);
		long time;
		if (object instanceof Date){
			Date date = (Date) object;
			time = date.getTime();
		}else if (object instanceof Timestamp){
			Timestamp timestamp = (Timestamp) object;
			time = timestamp.getTime();
		}else {
			time = (long) object;
		}
        boundedOutOfOrderTimestamps.nextTimestamp(time);
        return time;
    }
}
