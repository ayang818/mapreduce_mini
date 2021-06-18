package com.ayang818.mapreduce_frame_mini.base;

public class Pair {
    String key;
    ResultObj val;

    public Pair(String key, ResultObj val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ResultObj getVal() {
        return val;
    }

    public void setVal(ResultObj val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "key='" + key + '\'' +
                ", val=" + val +
                '}';
    }
}
