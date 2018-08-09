package test;

public class SunWordWithCount {
    public String word;
    public long count;

    public SunWordWithCount() {}

    public SunWordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
