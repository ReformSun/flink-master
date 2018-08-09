package model;

import java.util.HashMap;
import java.util.Map;

public class SunLine {
    private String key;
    private Map<String,String> map;


    public boolean add(String key,String value){
        if (map == null){
            this.map = new HashMap<>();
        }

        this.map.put(key,value);

        return true;
    }


    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    public String getValue(String key){
        if (this.map != null){
            return this.map.get(key);
        }

        return null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
