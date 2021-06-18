package com.ayang818.mapreduce_frame_mini;

import com.ayang818.mapreduce_frame_mini.base.Pair;
import com.ayang818.mapreduce_frame_mini.base.WorkerManager;
import com.ayang818.mapreduce_frame_mini.word_count.WCMapWorker;
import com.ayang818.mapreduce_frame_mini.word_count.WCReduceWorker;

import java.util.ArrayList;
import java.util.List;

public class WordCountBootstrap {
    public static void main(String[] args) {
        List<String> fileList = new ArrayList<>();
        String filePrefix = "/Users/bytedance/code/mapreduce_easy_frame/src/main/java/com/ayang818/mapreduce_frame_mini/data/";
        int fileNum = 3;
        for (int i = 1; i <= fileNum; i++) {
            fileList.add(String.format("%sfile%d.txt", filePrefix, i));
        }
        List<Pair> res = new WorkerManager(fileList, WCMapWorker.class, WCReduceWorker.class, 3, 5).start();
        res.forEach(System.out::println);
    }
}
