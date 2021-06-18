package com.ayang818.mapreduce_frame_mini.base;

import com.ayang818.mapreduce_frame_mini.word_count.WCMapWorker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultMapWorker implements IMapWorker {
    List<Pair> mappedResList = new ArrayList<>();
    /**
     * map 过程结束，手动调用为空
     */
    AtomicBoolean isDone = new AtomicBoolean(false);

    @Override
    public String preProcess(String fileName) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(fileName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    /**
     * map 前可以先进行 preProcess，用来读文件
     * @param key   the id of doc
     * @param value doc content
     */
    @Override
    public void map(String key, String value) {

    }

    @Override
    public void emit(Pair pair) {
        mappedResList.add(pair);
    }

    @Override
    public List<Pair> getMapResult(ShuffleCollectStrategy strategy) {
        List<Pair> res = new ArrayList<>();
        for (Pair pair : mappedResList) {
            if (strategy.shouldCollect(pair.getKey())) {
                res.add(pair);
            }
        }
        return res;
    }

    @Override
    public void done() {
        isDone.set(true);
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    public static void main(String[] args) {
        String[] res = new WCMapWorker().preProcess("/Users/bytedance/code/mapreduce_easy_frame/src/main/java/com/ayang818/mapreduce_frame_mini/data/file1.txt").split("\n");
        System.out.println(Arrays.toString(res));
    }
}
