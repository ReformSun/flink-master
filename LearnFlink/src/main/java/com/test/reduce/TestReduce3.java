package com.test.reduce;

import model.SunWordWithKey;
import org.apache.flink.api.common.functions.ReduceFunction;

public class TestReduce3 implements ReduceFunction<SunWordWithKey> {
    @Override
    public SunWordWithKey reduce(SunWordWithKey sunWordWithKey, SunWordWithKey t1) throws Exception {
        return null;
    }
}
