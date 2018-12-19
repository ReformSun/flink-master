package com.test.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class JsonObject {
    private Map<String, Object> map;

    public JsonObject() {
        map = new HashMap<>();
    }

    public JsonObject put(String key, Object value) {
        this.map.put(key,value);
        return this;
    }

    public String getString(String key) {
        Objects.requireNonNull(key);
        String cs = (String)this.map.get(key);
        return cs == null ? null : cs;
    }

    public Integer getInteger(String key) {
        Objects.requireNonNull(key);
        Integer number = (Integer)this.map.get(key);
        if (number == null) {
            return null;
        } else {
            return number instanceof Integer ? (Integer)number : number.intValue();
        }
    }

    public Long getLong(String key) {
        Objects.requireNonNull(key);
        Long number = (Long) this.map.get(key);
        if (number == null) {
            return null;
        } else {
            return number instanceof Long ? (Long)number : number.longValue();
        }
    }

    public Double getDouble(String key) {
        Objects.requireNonNull(key);
        Double number = (Double)this.map.get(key);
        if (number == null) {
            return null;
        } else {
            return number instanceof Double ? (Double)number : number.doubleValue();
        }
    }

    public Float getFloat(String key) {
        Objects.requireNonNull(key);
        Float number = (Float)this.map.get(key);
        if (number == null) {
            return null;
        } else {
            return number instanceof Float ? (Float)number : number.floatValue();
        }
    }

    public Boolean getBoolean(String key) {
        Objects.requireNonNull(key);
        return (Boolean)this.map.get(key);
    }
}
