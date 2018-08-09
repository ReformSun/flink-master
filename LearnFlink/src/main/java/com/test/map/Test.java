package com.test.map;

import model.SunWordWithKey;
import org.apache.flink.api.common.functions.MapFunction;

public class Test implements MapFunction<SunWordWithKey,String> {
//    @Override
//    public String map(String s) throws Exception {
//        System.out.println("ddd");
//        return "cc";
//    }
    @Override
    public String map(SunWordWithKey sunWordWithKey) throws Exception {
        return "cc";
    }
}
