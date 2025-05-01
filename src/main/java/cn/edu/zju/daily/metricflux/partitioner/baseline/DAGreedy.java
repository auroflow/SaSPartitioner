package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.partitioner.container.State;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DAGreedy<R extends Record<Integer>> extends MetricPartitioner<R> {
    private final State state;
    private final Map<Integer, Long> hotKeys; // key -> expiration timestamp
    private final int parallelism;

    public int hash(int key) {
        return (key) % parallelism;
    }

    public DAGreedy(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            int slide, // milliseconds
            int size, // milliseconds
            int numOfKeys,
            double initialPerf) {

        super(
                metricCollectorPort,
                numWorkers,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);

        parallelism = numWorkers;
        state = new State(size, slide, numWorkers, numOfKeys);
        hotKeys = new HashMap<>(numOfKeys);
    }

    public boolean isHot(Record<Integer> r) {
        boolean isHot = true;
        // returns 0 if not hot, 1 if hot, expirationTs if it just became hot
        long result = state.isHotDAG(r);
        if (result == 0) { // not hot in current window
            if (hotKeys.containsKey(r.getKey())
                    && hotKeys.get(r.getKey()) <= r.getTs()) { // hot in previous window (expired)
                hotKeys.remove(r.getKey());
                isHot = false;
            } else if (!hotKeys.containsKey(r.getKey())) {
                isHot = false;
            }

        } else if (result != 1) { // key just added to this window's hot keys
            if (!hotKeys.containsKey(r.getKey())) {
                hotKeys.put(r.getKey(), result);
            } else {
                hotKeys.put(r.getKey(), result);
            }
        }
        // if result == 1 then key was already hot in current window
        return isHot;
    }

    public void expireState(Record<Integer> r, boolean isHot) {
        state.updateExpired(r, isHot);
    }

    public int partition(Record<Integer> r, boolean isHot) {
        return (isHot) ? partitionHot(r) : hash(r.getKey());
    }

    public void updateState(Record<Integer> r, int worker) {
        state.update(r, worker);
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> out)
            throws Exception {
        boolean isHot;
        int worker;

        isHot = isHot(r);
        expireState(r, isHot);
        worker = partition(r, isHot);
        r.setHot(isHot);

        out.collect(new Tuple2<>(worker, r));

        updateState(r, worker);
        applyEffect(r.getKey(), worker, isHot);
    }

    public int partitionHot(Record<Integer> t) {
        int worker = 0;
        double bestCost = -Double.MAX_VALUE;
        int c1;
        double c2;
        double total;

        BitSet temp;

        for (int i = 0; i < parallelism; i++) {
            temp = state.keyfragmentation(t.getKey());
            if (temp.get(i)) {
                c1 = 1;
            } else {
                c1 = 0;
            }
            c2 = state.loadDAG(i);
            total = 0.5 * c1 - 0.5 * c2;
            if (total > bestCost) {
                bestCost = total;
                worker = i;
            }
        }
        return worker;
    }
}
