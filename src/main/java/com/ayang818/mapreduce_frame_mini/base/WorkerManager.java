package com.ayang818.mapreduce_frame_mini.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerManager implements IWorker {
    List<IMapWorker> mapWorkerList = new ArrayList<>();
    List<IReduceWorker> reduceWorkerList = new ArrayList<>();
    List<String> fileList;
    Class mWorkerKlass;
    Class rWorkerKlass;
    int mWorkerNum;
    int rWorkerNum;


    public WorkerManager(List<String> fileList, Class mWorkerKlass, Class rWorkerKlass, int mWorkerNum, int rWorkerNum) {
        this.fileList = fileList;
        this.mWorkerKlass = mWorkerKlass;
        this.rWorkerKlass = rWorkerKlass;
        this.mWorkerNum = mWorkerNum;
        this.rWorkerNum = rWorkerNum;
    }

    public WorkerManager(List<String> fileList, Class mWorkerKlass, Class rWorkerKlass) {
        this(fileList, mWorkerKlass, rWorkerKlass, 3, 3);
    }

    public List<Pair> start() {
        registerWorker();
        startAllMapWorker();
        startAllReduceWorker();
        return getResult();
    }

    private List<Pair> getResult() {
        System.out.println("start generate result~");
        // 打印结果
        List<Pair> output = new ArrayList<>();
        for (IReduceWorker reduceWorker : reduceWorkerList) {
            List<Pair> data = reduceWorker.getFinallyResult();
            output.addAll(data);
            System.out.printf("collect %s%n", data);
        }
        return output;
    }

    private void startAllMapWorker() {
        System.out.println("start all map worker");
        for (int i = 0; i < mapWorkerList.size(); i++) {
            IMapWorker mapWorker = mapWorkerList.get(i);
            String content = mapWorker.preProcess(fileList.get(i));
            mapWorker.map(String.valueOf(i), content);
        }
        System.out.println("end all map worker");
    }

    private void startAllReduceWorker() {
        System.out.println("start all reduce worker");
        for (int i = 0; i < reduceWorkerList.size(); i++) {
            IReduceWorker reduceWorker = reduceWorkerList.get(i);
            // 按照数组下标设置 worker shuffle 阶段的数据部分
            reduceWorker.shuffle(new HashShuffleCollectStrategy(reduceWorkerList.size(), i), mapWorkerList);
            // 等待 shuffle 阶段结束
            while (!reduceWorker.isShuffleDone()) {
            }
            List<Pair> shuffleResult = reduceWorker.getShuffleResult();
            Map<String, List<ResultObj>> aggregatedMap = new HashMap<>();
            // 将 shuffle 后的结果转化为 {{key: List[val, val, val...]}} 形式
            for (Pair pair : shuffleResult) {
                if (!aggregatedMap.containsKey(pair.getKey())) {
                    List<ResultObj> res = new ArrayList<>();
                    res.add(new ResultObj(1));
                    aggregatedMap.put(pair.getKey(), res);
                } else {
                    List<ResultObj> resultObjList = aggregatedMap.get(pair.getKey());
                    resultObjList.add(new ResultObj(1));
                }
            }
            // 进行 reduce
            for (Map.Entry<String, List<ResultObj>> entry : aggregatedMap.entrySet()) {
                reduceWorker.reduce(entry.getKey(), entry.getValue());
            }
            // 兼容该节点没有分配到数据的情况
            if (aggregatedMap.size() == 0) {
                reduceWorker.done();
            }
        }
        System.out.println("end all reduce worker");
    }

    private void registerWorker() {
        System.out.println("start register worker");
        try {
            for (int i = 0; i < mWorkerNum; i++) {
                mapWorkerList.add((IMapWorker) mWorkerKlass.newInstance());
            }
            for (int i = 0; i < rWorkerNum; i++) {
                reduceWorkerList.add((IReduceWorker) rWorkerKlass.newInstance());
            }
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        System.out.println("end register worker");
    }

    @Override
    public void done() {

    }

    @Override
    public boolean isDone() {
        return false;
    }
}
