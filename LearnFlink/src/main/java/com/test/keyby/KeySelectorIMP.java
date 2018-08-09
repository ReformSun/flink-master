package com.test.keyby;

import model.SunLine;
import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorIMP implements KeySelector<SunLine,String >{
    @Override
    public String getKey(SunLine sunLine) throws Exception {
        return null;
    }
}
