package com.test.filter;

import model.SunLine;
import org.apache.flink.api.common.functions.FilterFunction;

public class TestFilter2 implements FilterFunction<SunLine>{
    @Override
    public boolean filter(SunLine sunLine) throws Exception {

        if (sunLine.getValue("a").equals("ss")){
            return false;
        }else {
            return true;
        }
    }
}
