package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.partitioner.container.CardinalityWorker;
import java.util.ArrayList;
import java.util.List;

public abstract class CardinalityPartitioner<R extends Record<Integer>>
        extends MetricPartitioner<R> {
    private final double HASH_C = (Math.sqrt(5) - 1) / 2;
    private int nextUpdate;
    private final int slide;
    protected final int parallelism;

    protected List<CardinalityWorker> workersStats;

    public CardinalityPartitioner(
            int size,
            int slide,
            int parallelism,
            int metricCollectorPort,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double initialPerf) {

        super(
                metricCollectorPort,
                parallelism,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);

        this.parallelism = parallelism;
        this.slide = slide;
        nextUpdate = 0;

        workersStats = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            CardinalityWorker w = new CardinalityWorker(size, slide);
            workersStats.add(w);
        }
    }

    protected int hash1(int n) {
        return n % parallelism;
    }

    // https://www.geeksforgeeks.org/what-are-hash-functions-and-how-to-choose-a-good-hash-function/
    protected int hash2(int n) {
        double a = (n + 1) * HASH_C;
        return (int) Math.floor(parallelism * (a - (int) a));
    }

    protected void expireSlide(long now) {
        if (now >= nextUpdate) {
            for (CardinalityWorker w : workersStats) {
                w.expireOld(now);
                w.addNewSlide(nextUpdate);
            }
            nextUpdate += slide;
        }
    }

    protected void updateState(int worker, int key) {
        workersStats.get(worker).updateState(key);
    }
}
