package com.test.flatMap_1;

import java.io.Serializable;
import java.util.ArrayList;

public class SunFlatMapFunctionArgModel implements Serializable{
    private String TheLastCodeBlockOutput = "d";
    private String singleSeparator = "s";
    private String isPattern;
    private String inputKey;
    private ArrayList<String> keyList = new ArrayList(){{
//        arrayList
        add("d");
        add("c");
    }};

    public String getTheLastCodeBlockOutput() {
        return TheLastCodeBlockOutput;
    }

    public String getSingleSeparator() {
        return singleSeparator;
    }

    public String getIsPattern() {
        return isPattern;
    }

    public String getInputKey() {
        return inputKey;
    }

    public ArrayList<String> getKeyList() {
        return keyList;
    }
}
