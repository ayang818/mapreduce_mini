package com.ayang818.mapreduce_frame_mini.base;

import java.util.List;

public interface IReduceWorker extends IWorker {
    /**
     * @param strategy 洗牌收集策略
     * @param mapWorkerList 洗牌数据源
     */
    void shuffle(ShuffleCollectStrategy strategy, List<IMapWorker> mapWorkerList);
    void reduce(String key, List<ResultObj> resultObjList);
    boolean isShuffleDone();
    List<Pair> getShuffleResult();
    List<Pair> getFinallyResult();
}
