package cn.edu.zju.daily.metricflux.core.operator;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

public abstract class Reducer<K, P extends PartialResult<K>, F extends FinalResult<K>>
        extends RichAggregateFunction<P, P, F> implements Serializable {

    private final long metricUpdateInterval; // 10 s
    private long lastMetricUpdateTs = 0;

    protected Reducer(long metricUpdateInterval) {
        this.metricUpdateInterval = metricUpdateInterval;
    }

    /**
     * Updates manually the metrics if the route version has changed. This should be called in the
     * add method of the implementing subclass.
     *
     * @param r the record
     */
    protected void updateMetricsIfNecessary() {
        long now = System.currentTimeMillis();

        if (lastMetricUpdateTs == 0) {
            lastMetricUpdateTs = now;
            return;
        }

        if (now - lastMetricUpdateTs > metricUpdateInterval) {
            // The previous route window has ended, reporting metrics
            getTaskIOMetricGroup().updateManualGauges();
            double[] metrics = getMetrics(getTaskIOMetricGroup().getManualView());
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            if (metrics != null) {
                reportMetrics(subtask, metrics, now);
            }

            lastMetricUpdateTs = now;
        }
    }

    private TaskIOMetricGroup getTaskIOMetricGroup() {
        return ((InternalOperatorMetricGroup) getRuntimeContext().getMetricGroup())
                .getTaskIOMetricGroup();
    }

    protected double[] getMetrics(TaskIOMetricGroup.ManualView ioMetrics) {
        // currently only report non-idle time
        return new double[] {1000 - ioMetrics.getIdleTimeMsPerSecond()};
    }

    protected abstract void reportMetrics(int subtask, double[] metrics, long ts);
}
