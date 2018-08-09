package com.test.reduce;


import model.SunWordWithCount;
import org.apache.flink.api.common.functions.ReduceFunction;

public class TestReduce implements ReduceFunction<SunWordWithCount> {
    @Override
    public SunWordWithCount reduce(SunWordWithCount sunWordWithCount, SunWordWithCount t1) throws Exception {
        return new SunWordWithCount(sunWordWithCount.word, sunWordWithCount.count + t1.count);
    }
}
