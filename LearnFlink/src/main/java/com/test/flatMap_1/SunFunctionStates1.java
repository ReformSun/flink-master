package com.test.flatMap_1;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SunFunctionStates1 extends RichFlatMapFunction<Tuple2<Long,Long>, Tuple2<Long,Long>> {

    private transient ValueState<Tuple2<Long,Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {
        Tuple2<Long,Long> currentSum = sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += longLongTuple2.f1;

        sum.update(currentSum);

        if (currentSum.f0 >= 2){
            collector.collect(new Tuple2<Long, Long>(longLongTuple2.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor descriptor = new ValueStateDescriptor("average", TypeInformation.of(new TypeHint<Tuple2<Long,Long>>() {}), Tuple2.of(0L,0L));
        sum = getRuntimeContext().getState(descriptor);
    }
}
