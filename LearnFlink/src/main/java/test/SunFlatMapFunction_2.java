package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class SunFlatMapFunction_2 implements FlatMapFunction<String, SunWordWithKey> {
    String split;
    List<String> keys;

    public SunFlatMapFunction_2(String split,List<String> keys) {
        this.split = split;
        this.keys = keys;
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
