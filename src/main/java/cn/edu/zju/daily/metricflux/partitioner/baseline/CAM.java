package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Implementation of cAM algorithm
 *
 * <p>Nikos R. Katsipoulakis et al. A holistic view of stream partitioning costs. VLB'17
 */
public class CAM<R extends Record<Integer>> extends CardinalityPartitioner<R> {
    public CAM(
            int size,
            int slide,
            int parallelism,
            int metricCollectorPort,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double initialPerf) {
        super(
                size,
                slide,
                parallelism,
                metricCollectorPort,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf);
    }

    @Override
    protected void processElementInternal(
            R record,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> out)
            throws Exception {
        int recordId = record.getKey();
        int worker1 = hash1(recordId);
        int worker2 = hash2(recordId);
        int chosenWorker;

        if (workersStats.get(worker1).hasSeen(recordId)) {
            // worker1 saw this key before
            chosenWorker = worker1;
        } else if (workersStats.get(worker2).hasSeen(recordId)) {
            // worker2 saw this key before
            chosenWorker = worker2;
        } else {
            // otherwise choose based on tuple count-ups
            chosenWorker =
                    (workersStats.get(worker1).getLoad() < workersStats.get(worker2).getLoad())
                            ? worker1
                            : worker2;
        }
        updateState(chosenWorker, recordId);
        out.collect(new Tuple2<>(chosenWorker, record));
        applyEffect(recordId, chosenWorker, true);
    }
}
