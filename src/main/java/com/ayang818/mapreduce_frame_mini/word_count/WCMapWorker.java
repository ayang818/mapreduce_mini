package com.ayang818.mapreduce_frame_mini.word_count;

import com.ayang818.mapreduce_frame_mini.base.DefaultMapWorker;
import com.ayang818.mapreduce_frame_mini.base.Pair;
import com.ayang818.mapreduce_frame_mini.base.ResultObj;

public class WCMapWorker extends DefaultMapWorker {
    @Override
    public void map(String key, String value) {
        String[] wordList = value.split("\n");
        int len = wordList.length;
        for (int i = 0; i < len; i++) {
            emit(new Pair(wordList[i], new ResultObj<>(1)));
        }
        done();
    }
}
