package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import java.util.function.BiFunction;
import lombok.Setter;

public class StatRewardAssigner<K> implements BiFunction<K, Integer, Double> {

    @Setter private WorkerStatistics workerStatistics;
    private final int numWorkers;
    private static final double EPSILON = 1e-4;

    public StatRewardAssigner(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    @Override
    public Double apply(K k, Integer worker) {

        try {
            int load = workerStatistics.getCount(worker);
            double avgLoad = (double) workerStatistics.getTotalCount() / numWorkers;
            double score = (load - avgLoad) / (EPSILON + Math.max(load, avgLoad)); // [-1, 1]
            // the bigger, the better
            return (1 - score) / 2;
        } catch (NullPointerException e) {
            throw new RuntimeException("workerStatistics is not set");
        }
    }
}
