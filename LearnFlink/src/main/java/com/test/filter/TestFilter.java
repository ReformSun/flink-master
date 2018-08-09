package com.test.filter;

import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.common.functions.FilterFunction;

public class TestFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
        if (s.equals("dd")){
            return false;
        }
        return true;
    }
}
