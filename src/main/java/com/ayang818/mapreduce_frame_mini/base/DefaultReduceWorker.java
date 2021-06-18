package com.ayang818.mapreduce_frame_mini.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultReduceWorker implements IReduceWorker {
    List<Pair> shuffledResList = new ArrayList<>();
    List<Pair> finallyResult = new ArrayList<>();
    AtomicBoolean isDone = new AtomicBoolean(false);
    AtomicBoolean isShuffleDone = new AtomicBoolean(false);

    @Override
    public void shuffle(ShuffleCollectStrategy strategy, List<IMapWorker> mapWorkerList) {
        // TODO 分布式环境 isDone 可以为网络请求，目前先在本机模拟
        for (IMapWorker iMapWorker : mapWorkerList) {
            // TODO 等待 mapWorker 结束，目前为阻塞版本
            while (!iMapWorker.isDone()) {
            }
            shuffledResList.addAll(iMapWorker.getMapResult(strategy));
        }
        isShuffleDone.set(true);
    }

    @Override
    public void reduce(String key, List<ResultObj> resultObjList) {

    }

    @Override
    public boolean isShuffleDone() {
        return isShuffleDone.get();
    }

    @Override
    public List<Pair> getShuffleResult() {
        return shuffledResList;
    }

    @Override
    public List<Pair> getFinallyResult() {
        return finallyResult;
    }

    @Override
    public void done() {
        isDone.set(true);
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }
}
