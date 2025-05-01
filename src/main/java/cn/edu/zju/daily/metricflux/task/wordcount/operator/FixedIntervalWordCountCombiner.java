package cn.edu.zju.daily.metricflux.task.wordcount.operator;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.METRIC_REPORT_MAGIC_NUMBER;

import cn.edu.zju.daily.metricflux.partitioner.learning.MetricReportMessage;
import cn.edu.zju.daily.metricflux.partitioner.metrics.monitor.JobInfo;
import cn.edu.zju.daily.metricflux.task.wordcount.data.WordCountRecord;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Update metrics at a fixed interval. */
public class FixedIntervalWordCountCombiner extends WordCountCombiner {

    private static final Logger LOG = LoggerFactory.getLogger(FixedIntervalWordCountCombiner.class);

    /**
     * The name of a task which runs on the same host as the metric collector (in this case, the
     * partitioner).
     */
    private static final String METRIC_COLLECTOR_TASK_NAME = "partitioner";

    private final String jobManagerHost;
    private final int jobManagerPort;
    private final int metricCollectorPort;
    private final long metricUpdateInterval; // in milliseconds
    private Socket metricCollectorSocket;
    private ObjectOutputStream out;
    private long nextMetricUpdateTs;
    private long metricIntervalId;

    public FixedIntervalWordCountCombiner(
            int workloadRatio,
            String jobManagerHost,
            int jobManagerPort,
            int metricCollectorPort,
            Duration metricUpdateInterval) {
        super(workloadRatio);
        this.jobManagerHost = jobManagerHost;
        this.jobManagerPort = jobManagerPort;
        this.metricCollectorPort = metricCollectorPort;
        this.metricUpdateInterval = metricUpdateInterval.toMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        JobInfo jobInfo;
        try {
            jobInfo = new JobInfo(jobManagerHost, jobManagerPort);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to get job info from " + jobManagerHost + ":" + jobManagerPort, e);
        }
        String metricCollectorHost = jobInfo.getHost(METRIC_COLLECTOR_TASK_NAME, 0);
        while (!tryConnect(metricCollectorHost)) {
            Thread.sleep(1000);
        }
    }

    /**
     * Try to connect to the metric collector.
     *
     * @param metricCollectorHost The host of the metric collector.
     * @return True if the connection is successful, false otherwise.
     */
    private boolean tryConnect(String metricCollectorHost) {
        try {
            metricCollectorSocket = new Socket(metricCollectorHost, metricCollectorPort);
            out = new ObjectOutputStream(metricCollectorSocket.getOutputStream());
            out.writeInt(METRIC_REPORT_MAGIC_NUMBER);
            out.flush();
            return true;
        } catch (IOException e) {
            LOG.error(
                    "Failed to connect to the metric collector {}:{}, retrying...",
                    metricCollectorHost,
                    metricCollectorPort);
            return false;
        }
    }

    /**
     * Override the base class implementation to update and report metrics at a fixed interval.
     *
     * @param wordCountRecord the record
     * @param subtaskIndex
     */
    @Override
    protected void updateMetricsIfNecessary(WordCountRecord wordCountRecord, int subtaskIndex) {

        long now = System.currentTimeMillis();
        if (nextMetricUpdateTs == 0) {
            // Aligned so that metricMetricUpdateTs is divisible by metricUpdateInterval
            nextMetricUpdateTs = now + metricUpdateInterval - (now % metricUpdateInterval);
        }

        while (now >= nextMetricUpdateTs) {
            // Update metrics
            getTaskIOMetricGroup().updateManualGauges();
            double[] metrics = getMetrics(getTaskIOMetricGroup().getManualView());
            if (Objects.isNull(metrics)) {
                LOG.debug(
                        "[Route completed] subtask {} interval {} completed",
                        subtaskIndex,
                        metricIntervalId);
            } else {
                reportMetrics(nextMetricUpdateTs, subtaskIndex, metrics, metricUpdateInterval);
                LOG.debug(
                        "[Route completed] subtask {} interval {} completed, metrics: {}",
                        subtaskIndex,
                        metricIntervalId,
                        metrics);
            }

            nextMetricUpdateTs += metricUpdateInterval;
            metricIntervalId++;
        }
    }

    @Override
    protected void reportMetrics(
            long metricId, int subtaskIndex, double[] metrics, long routeDuration) {
        // send metrics to the reporter
        if (out == null) {
            LOG.error(
                    "Failed to send metrics {}:{} to the reporter: reporter socket not initialized.",
                    metricId,
                    subtaskIndex);
            return;
        }
        try {
            out.writeObject(
                    new MetricReportMessage(metricId, subtaskIndex, metrics, routeDuration));
            out.flush();
        } catch (IOException e) {
            LOG.error(
                    "Failed to send metrics {}:{} to the reporter: IO error",
                    metricId,
                    subtaskIndex,
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (out != null) {
            out.close();
        }
        if (metricCollectorSocket != null) {
            metricCollectorSocket.close();
        }
    }
}
