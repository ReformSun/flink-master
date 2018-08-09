package test;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SunReduceFunction_2 implements ReduceFunction<SunWordWithKey> {
    @Override
    public SunWordWithKey reduce(SunWordWithKey sunWordWithKey, SunWordWithKey t1) throws Exception {
        return sunWordWithKey;
    }
}
