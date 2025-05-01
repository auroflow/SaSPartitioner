package cn.edu.zju.daily.metricflux.utils.rate;

import static cn.edu.zju.daily.metricflux.utils.TimeUtils.rateToDelay;

import cn.edu.zju.daily.metricflux.core.socket.SocketServer;
import cn.edu.zju.daily.metricflux.partitioner.metrics.monitor.JobInfo;
import cn.edu.zju.daily.metricflux.utils.NetworkUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This limiter monitors the data rate of the Flink source operator, and cancels the source if the
 * rate is too low.
 */
public class VariableRateLimiter implements RateLimiter, Stoppable {

    private static final String COMBINER_TASK_NAME = "aggregate";
    private static final List<String> MONITORED_TASK_NAMES = List.of("aggregate");
    private static final int MAX_RATE_STEP = 10000;
    private static final double TARGET_BUSY_RATIO = 900;

    private static final Logger LOG = LoggerFactory.getLogger(VariableRateLimiter.class);

    private final long initialRate;
    private final long rateStep;
    private final long rateIntervalSeconds;
    private final long maxRate;
    private boolean stopped;
    private long previousRate;
    private long currentRate;
    private long currentRateStartTS;
    private long currentRateEndTS;
    private long totalCount;
    private final List<Long> warmupRates;
    private final List<Long> warmupIntervalsSeconds;
    private long lastRecordsSent;
    private long lastRecordsEmitted;
    private int warmupIndex;
    @Setter private SocketServer socketServer;
    private FlinkMonitor monitor = null;
    private String jobManagerHost;
    private int jobManagerPort;

    /**
     * Variable rate limiter.
     *
     * @param warmupRates warmup rates (tuples/s)
     * @param warmupIntervalsSeconds warmup intervals (s)
     * @param initialRate initial rate (tuples/s)
     * @param rateStep rate step (tuples/s)
     * @param maxRate max rate (tuples/s)
     */
    public VariableRateLimiter(
            List<Long> warmupRates,
            List<Long> warmupIntervalsSeconds,
            long initialRate,
            long rateStep,
            long rateIntervalSeconds,
            long maxRate,
            String jobManagerHost,
            int jobManagerPort) {
        if (initialRate < 0 || rateStep < 0 || rateIntervalSeconds < 0 || maxRate < 0) {
            throw new IllegalArgumentException("Rates must be non-negative.");
        }
        if (initialRate > maxRate) {
            throw new IllegalArgumentException(
                    "Initial rate must be less than or equal to max rate.");
        }
        this.warmupRates = warmupRates;
        this.warmupIntervalsSeconds = warmupIntervalsSeconds;
        this.initialRate = initialRate;
        this.rateStep = rateStep;
        this.rateIntervalSeconds = rateIntervalSeconds;
        this.maxRate = maxRate;
        this.warmupIndex = 0;
        this.previousRate = 0;
        this.currentRate = warmupRates.get(warmupIndex);
        this.lastRecordsSent = 0;
        this.currentRateStartTS = 0; // this will be set upon arrival of the first tuple
        this.currentRateEndTS = 0; // this will be set upon arrival of the first tuple
        LOG.info(
                "VariableRateLimiter created with first warmup rate {} tuples/s", this.currentRate);
        this.jobManagerHost = jobManagerHost;
        this.jobManagerPort = jobManagerPort;
    }

    private long getRate(long count) {
        totalCount++;
        long now = System.currentTimeMillis();
        if (currentRateStartTS == 0) {
            assert (warmupIndex == 0); // first tuple
            currentRateStartTS = now;
            currentRateEndTS = now + warmupIntervalsSeconds.get(0) * 1000;
            initializeFlinkMonitor();
            // [FLINK-32173] it seems the first call returns a wrong value, so we do it here and
            // discard the result
            getRecordsSentByFlinkCombiner();
        }

        if (now < currentRateEndTS) {
            // Still in the current rate interval
            return currentRate;
        }

        // Move to the next rate interval
        long recordsSent = getRecordsSentByFlinkCombiner();
        double actualSourceRate =
                (double) (totalCount - lastRecordsEmitted) * 1000 / (now - currentRateStartTS);
        double actualCombinerRate =
                (double) (recordsSent - lastRecordsSent) * 1000 / (now - currentRateStartTS);
        lastRecordsSent = recordsSent;
        lastRecordsEmitted = totalCount;

        LOG.info(
                "Summary: expected rate {} tuples/s, actual rate at source {} tuples/s, at combiner {} tuples/s",
                currentRate,
                actualSourceRate,
                actualCombinerRate);

        if (actualSourceRate < previousRate) {
            LOG.info("Backpressure observed, max rate = {}", previousRate);
            if (socketServer != null) {
                socketServer.stop();
            }
            if (monitor != null) {
                monitor.cancelRunningJob();
            }
            stopped = true;
            return previousRate;
        }

        previousRate = currentRate;
        currentRateStartTS = currentRateEndTS;
        warmupIndex++;
        long interval =
                warmupIndex < warmupIntervalsSeconds.size()
                        ? warmupIntervalsSeconds.get(warmupIndex)
                        : rateIntervalSeconds;
        currentRateEndTS = now + interval * 1000;
        if (warmupIndex > warmupRates.size()) {
            // warmup is over before last interval
            double maxBusyRatio =
                    monitor.getMaxBusyRatio(List.of("partitioner", "aggregate", "reduce"));
            long targetStep =
                    ((long) (currentRate * TARGET_BUSY_RATIO / maxBusyRatio) - currentRate) / 10;
            // targetStep should be divisible by rateStep
            targetStep = targetStep / rateStep * rateStep;
            targetStep = Math.min(targetStep, MAX_RATE_STEP);
            // Next current rate:
            // - If the busy ratio is not high enough, increase the rate by targetStep
            // - If the busy ratio is high enough, increase the rate by rateStep
            // - If the rate is already at maxRate, keep it
            currentRate = Math.min(currentRate + Math.max(rateStep, targetStep), maxRate);
            LOG.info(
                    "New rate {} tuples/s until: {} (max busy ratio = {}, target step = {})",
                    currentRate,
                    totalCount + currentRate * interval - 1,
                    maxBusyRatio,
                    targetStep);
        } else if (warmupIndex == warmupRates.size()) {
            // just finished warmup
            currentRate = initialRate;
            LOG.info("Warmup complete, rate now increases by {} tuples/s at minimum", rateStep);
        } else {
            // still in warmup
            currentRate = warmupRates.get(warmupIndex);
        }

        return currentRate;
    }

    @Override
    public long getDelayNanos(long count) {
        if (stopped) {
            // If stopped, emit the rest as soon as possible.
            return 0;
        }
        return rateToDelay(getRate(count));
    }

    private void initializeFlinkMonitor() {
        if (monitor == null) {
            monitor = new FlinkMonitor(jobManagerHost, jobManagerPort);
        }
    }

    private long getRecordsSentByFlinkCombiner() {
        return monitor.getNumRecordsSentByFlinkCombiner();
    }

    private static class FlinkMonitor {

        private final String jobManagerHost;
        private final int jobManagerPort;
        private final JobInfo jobInfo;
        private final String jobId;
        private final String jobUrl;
        private final String combinerVertexUrl;

        public FlinkMonitor(String jobManagerHost, int jobManagerPort) {
            this.jobManagerHost = jobManagerHost;
            this.jobManagerPort = jobManagerPort;

            jobInfo = new JobInfo(jobManagerHost, jobManagerPort);
            jobId = jobInfo.getJobId();
            String taskId = jobInfo.getTaskId(COMBINER_TASK_NAME);
            jobUrl = String.format("/jobs/%s", jobId);
            combinerVertexUrl = String.format("/jobs/%s/vertices/%s", jobId, taskId);
            LOG.info("jobUrl: {}", jobUrl);
            LOG.info("combinerVertexUrl: {}", combinerVertexUrl);
        }

        /**
         * Get the maximum busy ratio of all monitored tasks.
         *
         * @return busy ratio * 1000
         */
        public double getMaxBusyRatio(List<String> monitoredTaskNames) {

            List<String> monitoredTaskUrls = new ArrayList<>();
            for (String taskName : monitoredTaskNames) {
                String taskId = jobInfo.getTaskId(taskName);
                monitoredTaskUrls.add(
                        String.format(
                                "/jobs/%s/vertices/%s/subtasks/metrics?get=busyTimeMsPerSecond",
                                jobId, taskId));
            }
            LOG.debug("Monitored task urls: {}", monitoredTaskUrls);

            double maxBusyRatio = 0.0;
            try {
                for (String url : monitoredTaskUrls) {
                    JSONArray task =
                            NetworkUtils.httpGetJson(jobManagerHost, jobManagerPort, url).toArray();
                    double busyRatio = task.getJSONObject(0).getDouble("max");
                    if (!Double.isNaN(busyRatio)) {
                        maxBusyRatio = Math.max(maxBusyRatio, busyRatio);
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to get busy ratio", e);
                return -1;
            }
            return maxBusyRatio;
        }

        public long getNumRecordsSentByFlinkCombiner() {
            JSONObject vertex =
                    NetworkUtils.httpGetJson(jobManagerHost, jobManagerPort, combinerVertexUrl)
                            .toObject();
            return vertex.getJSONObject("aggregated")
                    .getJSONObject("metrics")
                    .getJSONObject("read-records")
                    .getLong("sum");
        }

        public void cancelRunningJob() {
            int code =
                    NetworkUtils.httpPatch(jobManagerHost, jobManagerPort, jobUrl + "?mode=cancel");
            if (code != 202) {
                LOG.error("Failed to cancel job, response code: {}", code);
            }
        }
    }
}
