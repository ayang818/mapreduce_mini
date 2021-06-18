package com.ayang818.mapreduce_frame_mini.base;

public class ResultObj<E> {
    E val;

    public ResultObj(E val) {
        this.val = val;
    }

    public E getVal() {
        return val;
    }

    public void setVal(E val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return "ResultObj{" +
                "val=" + val +
                '}';
    }
}
