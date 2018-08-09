package model;

public class SunWordWithKey {
    private String value;
    private String key;

    public SunWordWithKey() {
    }

    public SunWordWithKey(String value, String key) {
        this.value = value;
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override

    public String toString() {
        return key + ":" + value;
    }
}
