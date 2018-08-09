package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class SunFlatMapFunction implements FlatMapFunction<String, SunWordWithCount>{
    @Override
    public void flatMap(String s, Collector<SunWordWithCount> collector) throws Exception {
        System.out.println("SunFlatMapFunction:" + s);
        for (String word : s.split("\\s")) {
            collector.collect(new SunWordWithCount(word, 1L));
        }
    }
}
