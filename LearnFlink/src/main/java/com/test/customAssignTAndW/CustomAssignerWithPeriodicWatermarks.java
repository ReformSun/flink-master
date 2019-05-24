package com.test.customAssignTAndW;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

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
        long time = (long)element.getField(index);
        boundedOutOfOrderTimestamps.nextTimestamp(time);
        return time;
    }
}
