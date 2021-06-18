package com.ayang818.mapreduce_frame_mini.base;

import java.util.List;

public interface IMapWorker extends IWorker {

    String preProcess(String fileName);

    /**
     * @param key   the id of doc
     * @param value doc content
     */
    void map(String key, String value);

    void emit(Pair pair);

    List<Pair> getMapResult(ShuffleCollectStrategy strategy);
}
