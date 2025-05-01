package cn.edu.zju.daily.metricflux.partitioner.learning;

import java.io.Serializable;
import lombok.Data;

/** Message for the combiners to report metrics to the {@link MetricCollector}. */
@Data
public class MetricReportMessage implements Serializable {

    /** Metric identifier. Increments with time. */
    private final long metricId;

    private final int worker;
    private final double[] metrics;

    /** The duration of this route metricWindow in millis. */
    private final long routeWindowDuration;
}
