package com.ayang818.mapreduce_frame_mini.base;

/**
 * TODO 如何处理数据倾斜问题
 * 洗牌策略：收集某一类 (key,value) 到某一个 ReduceWorker
 */
public interface ShuffleCollectStrategy {
    boolean shouldCollect(Object obj);
}

class HashShuffleCollectStrategy implements ShuffleCollectStrategy {
    int modVal;
    int equalVal;
    /**
     * @param modVal 通常为 reduceWorker 的数量
     */
    public HashShuffleCollectStrategy(int modVal, int equalVal) {
        this.modVal = modVal;
        this.equalVal = equalVal;
    }

    @Override
    public boolean shouldCollect(Object obj) {
        // 值域: 0 - (this.modVal - 1)
        int afterHashVal = Math.abs(obj.hashCode() % this.modVal);
        if (afterHashVal == (int) this.equalVal) {
            return true;
        }
        return false;
    }
}