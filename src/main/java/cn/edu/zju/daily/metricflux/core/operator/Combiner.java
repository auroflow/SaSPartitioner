package cn.edu.zju.daily.metricflux.core.operator;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.PENDING_UPDATE_ROUTE_VERSION;

import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import cn.edu.zju.daily.metricflux.core.data.Record;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup.ManualView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Combiner<K, R extends Record<K>, P extends PartialResult<K>>
        extends RichAggregateFunction<Tuple2<Integer, R>, Map<K, P>, Map<K, P>>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Combiner.class);

    private long currentRouteVersion = 0;
    private long currentRouteWindowStart = 0L;

    /**
     * Manually update the metrics if the route version has changed, and report the metrics if a
     * meaningful metric window has ended. This should be called in the add method of the
     * implementing subclass.
     *
     * @param r the record
     */
    protected void updateMetricsIfNecessary(R r, int subtaskIndex) {

        long newRouteVersion;
        if (Objects.isNull(r)) {
            // null indicates previous route version has ended
            newRouteVersion = PENDING_UPDATE_ROUTE_VERSION;
        } else {
            newRouteVersion = r.getRouteVersion();
        }

        if (newRouteVersion != currentRouteVersion) {
            long now = System.currentTimeMillis();
            // Two cases:
            // - current route version is not PENDING, and new route version is PENDING
            // - current route version is not PENDING, and new route version is not PENDING
            if (currentRouteVersion != PENDING_UPDATE_ROUTE_VERSION) {
                // The previous route window has ended, reporting metrics
                getTaskIOMetricGroup().updateManualGauges();

                long duration = now - currentRouteWindowStart;

                double[] metrics = getMetrics(getTaskIOMetricGroup().getManualView());
                if (Objects.isNull(metrics)) {
                    LOG.debug(
                            "[Route completed] subtask {} route version {} completed in {} ms",
                            subtaskIndex,
                            currentRouteVersion,
                            duration);
                } else {
                    reportMetrics(currentRouteVersion, subtaskIndex, metrics, duration);
                    LOG.debug(
                            "[Route completed] subtask {} route version {} completed in {} ms, metrics: {}",
                            subtaskIndex,
                            currentRouteVersion,
                            duration,
                            metrics);
                }
            } else {
                // The route version is updated, only reset the metrics
                getTaskIOMetricGroup().updateManualGauges();
                currentRouteWindowStart = now;
                LOG.debug(
                        "[Route started] subtask {} route version {} started",
                        subtaskIndex,
                        newRouteVersion);
            }

            currentRouteVersion = newRouteVersion;
        }
    }

    protected final TaskIOMetricGroup getTaskIOMetricGroup() {

        return ((InternalOperatorMetricGroup) getRuntimeContext().getMetricGroup())
                .getTaskIOMetricGroup();
    }

    /**
     * Get the metric to report from the I/O metric group.
     *
     * @param ioMetrics the I/O metric group
     * @return the metric to report, or NaN if no need to report
     */
    protected abstract double[] getMetrics(ManualView ioMetrics);

    /**
     * Report the metrics. Subclass should define how.
     *
     * @param routeVersion the route version
     * @param subtaskIndex the subtask index
     * @param metrics the metrics of the current subtask to report
     * @param routeDuration the duration of the route in millis
     */
    protected abstract void reportMetrics(
            long routeVersion, int subtaskIndex, @NonNull double[] metrics, long routeDuration);
}
