package cn.edu.zju.daily.metricflux.task.tdigest.operator;

import cn.edu.zju.daily.metricflux.core.operator.Reducer;
import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestState;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDigestReducer extends Reducer<Integer, TDigestState, TDigestState> {

    public TDigestReducer(long metricUpdateInterval) {
        super(metricUpdateInterval);
    }

    @Override
    public TDigestState createAccumulator() {
        return null;
    }

    @Override
    public TDigestState add(TDigestState value, TDigestState accumulator) {
        assert (value != null);

        updateMetricsIfNecessary();

        if (accumulator != null) {
            accumulator.merge(value);
        } else {
            accumulator = value;
        }
        return accumulator;
    }

    @Override
    public TDigestState getResult(TDigestState accumulator) {
        return accumulator;
    }

    @Override
    public TDigestState merge(TDigestState a, TDigestState b) {
        if (a == null) {
            return b;
        }
        return add(a, b);
    }

    @Override
    protected void reportMetrics(int subtask, double[] metrics, long ts) {
        LOG.info("Reducer subtask {} metrics: {}", subtask, metrics);
    }
}
