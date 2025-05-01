package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.utils.HashUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DriftingKeyPartitioner<R extends Record<Integer>> extends MetricPartitioner<R> {

    private final int numWorkers;
    private final int driftingKey;

    public DriftingKeyPartitioner(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double initialPerf,
            int driftingKey) {
        super(
                metricCollectorPort,
                numWorkers,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);

        this.numWorkers = numWorkers;
        this.driftingKey = driftingKey;
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> collector)
            throws Exception {
        int key = r.getKey();
        int worker;
        if (key == driftingKey) {
            worker = 0;
        } else {
            worker = Math.floorMod(HashUtils.hash(Integer.hashCode(key)), numWorkers - 1) + 1;
        }
        collector.collect(new Tuple2<>(worker, r));
        applyEffect(r.getKey(), worker, false);
    }
}
