package cn.edu.zju.daily.metricflux.utils.rate;

import static cn.edu.zju.daily.metricflux.utils.TimeUtils.rateToDelay;

import cn.edu.zju.daily.metricflux.partitioner.metrics.monitor.JobInfo;
import cn.edu.zju.daily.metricflux.utils.NetworkUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;

/**
 * This limiter provides TEST and ADAPTIVE modes, targeting training & testing scenarios. ADAPTIVE
 * mode is used for training, during which the data rate adjusts based on the busy ratio of the
 * monitored tasks, maintaining a moderate workload. TEST mode is used for testing the maximum
 * throughput, where the data rate increases until backpressure is observed.
 */
@Slf4j
public class AdaptiveRateLimiter implements RateLimiter {

    private enum Mode {

        /** Adaptive rate limiter. */
        ADAPTIVE,
        /** Test max throughput. */
        TEST
    }

    private static final List<String> MONITORED_TASK_NAMES = List.of("aggregate", "reduce");
    private static final String COMBINER_TASK_NAME = "reduce";
    private static final long ADAPTIVE_RATE_INTERVAL_SECS = 30;
    private static final long TEST_RATE_INTERVAL_SECS = 120;
    private static final double BUSY_RATIO_UPPER_THRESHOLD = 900; // 800
    private static final double BUSY_RATIO_LOWER_THRESHOLD = 700; // 500

    private final long baseRateStep;
    private final Clock clock;
    private final long firstAdaptiveDurationSec;
    private final long nextAdaptiveDurationSec;
    private final boolean doTest;
    private final boolean testOnlyOnce;
    private long currentRate;
    private long currentRateStartTS;
    private long currentRateEndTS;
    private long nextTestTS;
    private FlinkMonitor monitor = null;
    private final String jobManagerHost;
    private final int jobManagerPort;
    private Mode mode;
    private long prevAdaptiveRate;
    private long totalCount;
    private long lastRecordsEmitted;
    private long prevTestRate;
    private boolean canceled;

    /**
     * Adaptive rate limiter.
     *
     * @param initialRate initial rate (tuples/s)
     * @param baseRateStep base rate step (tuples/s)
     * @param jobManagerHost Flink job manager host
     * @param jobManagerPort Flink job manager port
     * @param firstAdaptiveDurationSec duration of the first adaptive mode (s)
     * @param nextAdaptiveDurationSec duration of the next adaptive mode (s)
     * @param doTest whether to test max throughput periodically
     * @param testOnlyOnce whether to stop the experiment after the first test
     */
    public AdaptiveRateLimiter(
            long initialRate,
            long baseRateStep,
            String jobManagerHost,
            int jobManagerPort,
            long firstAdaptiveDurationSec,
            long nextAdaptiveDurationSec,
            boolean doTest,
            boolean testOnlyOnce) {
        this(
                initialRate,
                baseRateStep,
                jobManagerHost,
                jobManagerPort,
                firstAdaptiveDurationSec,
                nextAdaptiveDurationSec,
                doTest,
                testOnlyOnce,
                Clock.systemUTC());
    }

    /**
     * Adaptive rate limiter.
     *
     * @param initialRate initial rate (tuples/s)
     * @param rateStep rate step in ADAPTIVE mode, minimum rate step in TEST mode (tuples/s)
     * @param jobManagerHost Flink job manager host
     * @param jobManagerPort Flink job manager port
     * @param firstAdaptiveDurationSec duration of the first adaptive mode (s)
     * @param nextAdaptiveDurationSec duration of the next adaptive mode (s)
     * @param doTest whether to test max throughput periodically
     * @param testOnlyOnce whether to stop the experiment after the first test
     * @param clock clock
     */
    AdaptiveRateLimiter(
            long initialRate,
            long baseRateStep,
            String jobManagerHost,
            int jobManagerPort,
            long firstAdaptiveDurationSec,
            long nextAdaptiveDurationSec,
            boolean doTest,
            boolean testOnlyOnce,
            Clock clock) {
        if (initialRate < 0 || baseRateStep < 0) {
            throw new IllegalArgumentException("Rates must be non-negative.");
        }
        this.baseRateStep = baseRateStep;
        this.currentRate = initialRate;
        this.currentRateStartTS = 0;
        this.currentRateEndTS = 0; // this will be set upon arrival of the first tuple
        this.nextTestTS = 0;
        LOG.info(
                "VariableRateLimiter created with first warmup rate {} tuples/s", this.currentRate);
        this.jobManagerHost = jobManagerHost;
        this.jobManagerPort = jobManagerPort;
        this.doTest = doTest;
        this.testOnlyOnce = testOnlyOnce;
        this.clock = clock;
        this.mode = Mode.ADAPTIVE;
        this.firstAdaptiveDurationSec = firstAdaptiveDurationSec;
        this.nextAdaptiveDurationSec = nextAdaptiveDurationSec;
        this.prevAdaptiveRate = 0;
        this.prevTestRate = 0;
    }

    private long getRate(long count) {
        totalCount++;

        if (canceled) {
            return 0;
        }

        long now = clock.millis();

        // Initialization
        if (currentRateEndTS == 0) {
            currentRateStartTS = now;
            currentRateEndTS = now + ADAPTIVE_RATE_INTERVAL_SECS * 1000;
            nextTestTS = now + firstAdaptiveDurationSec * 1000;
            initializeFlinkMonitor();
            // [FLINK-32173] it seems the first call returns a wrong value, so we do it here and
            // discard the result
            monitor.getMaxBusyRatio();
            LOG.info("Initial rate: {}", currentRate);
        }

        if (now > currentRateEndTS) {
            double busyRatio = monitor.getMaxBusyRatio();
            if (busyRatio == -1) {
                LOG.info("Failed to get busy ratio, keep current rate {}", currentRate);
                return currentRate;
            }

            if (this.mode == Mode.ADAPTIVE) {

                long rateStep = getNextAdaptiveRateStep();
                if (doTest && now > nextTestTS && busyRatio < BUSY_RATIO_UPPER_THRESHOLD) {
                    // Go to test mode if it's time and we are not back-pressuring
                    this.mode = Mode.TEST;
                    // Prepare for test
                    prevAdaptiveRate = currentRate;
                    prevTestRate = 0;
                    currentRateStartTS = now;
                    currentRateEndTS = now + TEST_RATE_INTERVAL_SECS * 1000;
                    lastRecordsEmitted = totalCount;
                    currentRate += rateStep;
                    LOG.info("Adaptive mode: switching to test mode, rate = {}", currentRate);
                } else {
                    // otherwise, stay in adaptive mode, and adaptively adjust rate
                    if (busyRatio > BUSY_RATIO_UPPER_THRESHOLD && currentRate - rateStep > 0) {
                        LOG.info(
                                "Adaptive mode: Max busy ratio = {} > {}, rate switches from {} to {}",
                                busyRatio,
                                BUSY_RATIO_UPPER_THRESHOLD,
                                currentRate,
                                currentRate - rateStep);
                        currentRate -= rateStep;
                    } else if (busyRatio < BUSY_RATIO_LOWER_THRESHOLD) {
                        LOG.info(
                                "Adaptive mode: Max busy ratio = {} < {}, rate switches from {} to {}",
                                busyRatio,
                                BUSY_RATIO_LOWER_THRESHOLD,
                                currentRate,
                                currentRate + rateStep);
                        currentRate += rateStep;
                    } else {
                        LOG.info(
                                "Adaptive mode: Max busy ratio = {} in [{}, {}], keep current rate {}",
                                busyRatio,
                                BUSY_RATIO_LOWER_THRESHOLD,
                                BUSY_RATIO_UPPER_THRESHOLD,
                                currentRate);
                    }
                    currentRateStartTS = now;
                    currentRateEndTS = now + ADAPTIVE_RATE_INTERVAL_SECS * 1000;
                }
            } else { // this.mode == Mode.TEST
                long rateStep = getNextTestRateStep(busyRatio);
                if (isBackPressured(now)) {
                    if (currentRate - prevTestRate != baseRateStep) {
                        LOG.warn(
                                "WARNING: the last rate step {} is not base rate step {}. This measurement might not "
                                        + "be accurate!",
                                currentRate - prevTestRate,
                                baseRateStep);
                    }
                    if (testOnlyOnce) {
                        LOG.info("Test mode: Back-pressured, max rate = {}", prevTestRate);
                        LOG.info("Ending experiment.");
                        monitor.cancelRunningJob();
                        canceled = true;
                        return 0;
                    }
                    LOG.info(
                            "Test mode: Back-pressured, max rate = {}, switching back to adaptive mode at rate {}",
                            prevTestRate,
                            prevAdaptiveRate);
                    currentRate = prevAdaptiveRate;
                    currentRateStartTS = now;
                    currentRateEndTS = now + ADAPTIVE_RATE_INTERVAL_SECS * 1000;
                    this.mode = Mode.ADAPTIVE;
                    nextTestTS = now + nextAdaptiveDurationSec * 1000;
                } else {
                    prevTestRate = currentRate;
                    currentRate += rateStep;
                    LOG.info("Test mode: busy ratio {}, new test rate {}", busyRatio, currentRate);
                    currentRateStartTS = now;
                    currentRateEndTS = now + TEST_RATE_INTERVAL_SECS * 1000;
                }
            }
        }

        return currentRate;
    }

    private long getNextAdaptiveRateStep() {
        // assumes that the current & next mode is ADAPTIVE
        // rate step should be at least 1/50 * currentRate and divisible by baseRateStep
        long rateStep = Math.max(currentRate / 50, baseRateStep);
        return rateStep - rateStep % baseRateStep;
    }

    private long getNextTestRateStep(double busyRatio) {
        // assume that the current & next mode is TEST
        long targetStep = ((long) (currentRate * 900 / busyRatio) - currentRate) / 10;
        targetStep -= targetStep % baseRateStep;
        return Math.max(targetStep, baseRateStep);
    }

    private boolean isBackPressured(long now) {
        double actualSourceRate =
                (double) (totalCount - lastRecordsEmitted) * 1000 / (now - currentRateStartTS);
        lastRecordsEmitted = totalCount;

        LOG.info(
                "Summary: expected rate {} tuples/s, actual rate at source {} tuples/s",
                currentRate,
                actualSourceRate);

        return actualSourceRate < prevTestRate;
    }

    @Override
    public long getDelayNanos(long count) {
        return rateToDelay(getRate(count));
    }

    private void initializeFlinkMonitor() {
        if (monitor == null) {
            monitor = new FlinkMonitor(jobManagerHost, jobManagerPort);
        }
    }

    private static class FlinkMonitor {

        private final String jobManagerHost;
        private final int jobManagerPort;
        private String jobUrl;
        private List<String> monitoredTaskUrls;
        private String combinerVertexUrl;

        public FlinkMonitor(String jobManagerHost, int jobManagerPort) {
            this.jobManagerHost = jobManagerHost;
            this.jobManagerPort = jobManagerPort;
        }

        /**
         * Get the maximum busy ratio of all monitored tasks.
         *
         * @return busy ratio * 1000
         */
        public double getMaxBusyRatio() {

            if (jobUrl == null) {
                JobInfo jobInfo = new JobInfo(jobManagerHost, jobManagerPort);
                String jobId = jobInfo.getJobId();
                this.monitoredTaskUrls = new ArrayList<>();
                for (String taskName : MONITORED_TASK_NAMES) {
                    String taskId = jobInfo.getTaskId(taskName);
                    monitoredTaskUrls.add(
                            String.format(
                                    "/jobs/%s/vertices/%s/subtasks/metrics?get=busyTimeMsPerSecond",
                                    jobId, taskId));
                }
                String combinerTaskId = jobInfo.getTaskId(COMBINER_TASK_NAME);
                combinerVertexUrl = String.format("/jobs/%s/vertices/%s", jobId, combinerTaskId);
                jobUrl = String.format("/jobs/%s", jobId);
                LOG.info("jobUrl: {}", jobUrl);
                LOG.info("Monitored task urls: {}", monitoredTaskUrls);
            }

            double maxBusyRatio = 0.0;
            try {
                for (String url : monitoredTaskUrls) {
                    JSONArray task =
                            NetworkUtils.httpGetJson(jobManagerHost, jobManagerPort, url).toArray();
                    double busyRatio = task.getJSONObject(0).getDouble("max");
                    maxBusyRatio = Math.max(maxBusyRatio, busyRatio);
                }
            } catch (Exception e) {
                LOG.error("Failed to get busy ratio", e);
                return -1;
            }
            return maxBusyRatio;
        }

        public void cancelRunningJob() {

            JobInfo jobInfo = new JobInfo(jobManagerHost, jobManagerPort);
            String jobId = jobInfo.getJobId();
            String jobUrl = String.format("/jobs/%s", jobId);

            int code =
                    NetworkUtils.httpPatch(jobManagerHost, jobManagerPort, jobUrl + "?mode=cancel");
            if (code != 202) {
                LOG.error("Failed to cancel job, response code: {}", code);
            }
        }
    }
}
