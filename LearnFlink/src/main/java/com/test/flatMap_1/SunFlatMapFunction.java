package com.test.flatMap_1;

import model.SunWordWithKey;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class SunFlatMapFunction implements FlatMapFunction<String, SunWordWithKey> {
    String split;
    List<String> keys;

    public SunFlatMapFunction(com.test.flatMap_1.SunFlatMapFunctionArgModel sunFlatMapFunctionArgModel) {
        this.split = sunFlatMapFunctionArgModel.getSingleSeparator();
        this.keys = sunFlatMapFunctionArgModel.getKeyList();
    }

    @Override
    public void flatMap(String s, Collector<SunWordWithKey> collector) throws Exception {
        int i = 0;
        for(String ss:s.split(split)){
            collector.collect(new SunWordWithKey(ss,this.keys.get(i)));
            i ++;
        }
    }
}
