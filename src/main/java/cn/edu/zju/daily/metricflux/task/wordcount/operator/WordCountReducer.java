package cn.edu.zju.daily.metricflux.task.wordcount.operator;

import cn.edu.zju.daily.metricflux.core.operator.Reducer;
import cn.edu.zju.daily.metricflux.task.wordcount.data.WordCountState;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WordCountReducer extends Reducer<Integer, WordCountState, WordCountState> {

    public WordCountReducer(long metricUpdateInterval) {
        super(metricUpdateInterval);
    }

    @Override
    public WordCountState createAccumulator() {
        return null;
    }

    @Override
    public WordCountState add(WordCountState value, WordCountState accumulator) {
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
    public WordCountState getResult(WordCountState accumulator) {
        return accumulator;
    }

    @Override
    public WordCountState merge(WordCountState a, WordCountState b) {
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
