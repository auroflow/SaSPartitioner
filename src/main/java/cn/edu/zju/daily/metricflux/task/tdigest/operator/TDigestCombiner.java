package cn.edu.zju.daily.metricflux.task.tdigest.operator;

import cn.edu.zju.daily.metricflux.core.operator.Combiner;
import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestRecord;
import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestState;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup.ManualView;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDigestCombiner extends Combiner<Integer, TDigestRecord, TDigestState> {

    private static final int HISTOGRAM_SIZE = 100000;

    private final int workloadRatio;
    private static final Clock clock = SystemClock.getInstance();
    private static final Logger LOG = LoggerFactory.getLogger(TDigestCombiner.class);

    public TDigestCombiner(int workloadRatio) {
        if (workloadRatio < 0) {
            throw new IllegalArgumentException("Workload ratio must be non-negative.");
        }
        this.workloadRatio = workloadRatio;
    }

    public TDigestCombiner() {
        this(1);
    }

    @Override
    public Map<Integer, TDigestState> createAccumulator() {
        return new HashMap<>();
    }

    /**
     * @param value The value to add: (partitionId, record)
     * @param accumulator The accumulator to add the value to: (key, state)
     * @return
     */
    @Override
    public Map<Integer, TDigestState> add(
            Tuple2<Integer, TDigestRecord> value, Map<Integer, TDigestState> accumulator) {
        TDigestRecord record = value.f1;
        updateMetricsIfNecessary(record, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());

        if (record == null) {
            // null indicates previous route version has ended
            return accumulator;
        }

        int key = record.getKey();
        for (int workload = 0; workload < workloadRatio; workload++) {
            if (!accumulator.containsKey(key)) {
                TDigestState newState = new TDigestState(key);
                newState.count(record.getValues());
                newState.setHot(record.isHot());
                accumulator.put(key, newState);
            } else {
                TDigestState state = accumulator.get(key);
                state.count(record.getValues());
                state.setHot(state.isHot() || record.isHot());
            }
        }
        return accumulator;
    }

    @Override
    public Map<Integer, TDigestState> getResult(Map<Integer, TDigestState> accumulator) {
        return accumulator;
    }

    @Override
    public Map<Integer, TDigestState> merge(
            Map<Integer, TDigestState> a, Map<Integer, TDigestState> b) {

        for (int workload = 0; workload < workloadRatio; workload++) {
            for (TDigestState state : b.values()) {
                int key = state.getKey();
                if (!a.containsKey(key)) {
                    a.put(key, state);
                } else {
                    a.get(key).merge(state);
                }
            }
        }
        return a;
    }

    @Override
    protected double[] getMetrics(ManualView ioMetrics) {
        //                return new double[] {
        //                    ioMetrics.getIdleTimeMsPerSecond(),
        //                    ioMetrics.getBackPressuredTimeMsPerSecond(),
        //                    ioMetrics.getBusyTimeMsPerSecond()
        //                };
        return new double[] {ioMetrics.getBusyTimeMsPerSecond()};
    }

    @Override
    protected void reportMetrics(
            long routeVersion, int subtaskIndex, double[] metrics, long routeDuration) {
        // Do not report
        LOG.info(
                "Route {} subtask {} completed in {} ms, metrics: {}",
                routeVersion,
                subtaskIndex,
                routeDuration,
                metrics);
    }
}
