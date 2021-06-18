package com.ayang818.mapreduce_frame_mini.word_count;

import com.ayang818.mapreduce_frame_mini.base.DefaultReduceWorker;
import com.ayang818.mapreduce_frame_mini.base.Pair;
import com.ayang818.mapreduce_frame_mini.base.ResultObj;

import java.util.List;

public class WCReduceWorker extends DefaultReduceWorker {

    /**
     * reduceWorker 进行 reduce 前需要先进行 shuffle()
     *
     * @param key
     * @param resultObjList
     */
    @Override
    public void reduce(String key, List<ResultObj> resultObjList) {
        // 添加次数
        getFinallyResult().add(new Pair(key, new ResultObj(resultObjList.size())));
        done();
    }
}
