package test;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SunReduceFunction implements ReduceFunction<SunWordWithCount> {
    @Override
    public SunWordWithCount reduce(SunWordWithCount sunWordWithCount, SunWordWithCount t1) throws Exception {
        return new SunWordWithCount(sunWordWithCount.word, sunWordWithCount.count + t1.count);
    }
}
