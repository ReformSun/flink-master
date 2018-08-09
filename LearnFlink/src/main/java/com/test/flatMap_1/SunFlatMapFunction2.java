package com.test.flatMap_1;

import model.SunLine;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class SunFlatMapFunction2 implements FlatMapFunction<String , SunLine>{
    String split;
    List<String> keys;

    public SunFlatMapFunction2(String split, List<String> keys) {
        this.split = split;
        this.keys = keys;
    }

    @Override
    public void flatMap(String s, Collector<SunLine> collector) throws Exception {

        SunLine sunLine = new SunLine();

        int i = 0;
        for(String ss:s.split(split)){
            sunLine.add(keys.get(i),ss);
            i ++;
        }

        collector.collect(sunLine);

    }
}
