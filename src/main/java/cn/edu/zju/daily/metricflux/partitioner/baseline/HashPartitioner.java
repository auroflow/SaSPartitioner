package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.utils.HashUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class HashPartitioner<R extends Record<Integer>> extends MetricPartitioner<R> {

    private final int numWorkers;

    public HashPartitioner(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double initialPerf) {
        super(
                metricCollectorPort,
                numWorkers,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);

        this.numWorkers = numWorkers;
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> collector)
            throws Exception {
        int worker = Math.floorMod(HashUtils.hash(r.getKey().hashCode()), numWorkers);
        collector.collect(new Tuple2<>(worker, r));
        applyEffect(r.getKey(), worker, false);
    }
}
