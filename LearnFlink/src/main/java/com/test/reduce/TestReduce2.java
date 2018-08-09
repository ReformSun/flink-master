package com.test.reduce;

import model.SunLine;
import org.apache.flink.api.common.functions.ReduceFunction;

public class TestReduce2 implements ReduceFunction<SunLine> {
    @Override
    public SunLine reduce(SunLine sunLine, SunLine t1) throws Exception {
        System.out.println("dd");
        return new SunLine();
    }
}
