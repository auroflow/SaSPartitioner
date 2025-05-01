package cn.edu.zju.daily.metricflux.utils.rate;

import cn.edu.zju.daily.metricflux.partitioner.metrics.monitor.JobInfo;
import cn.edu.zju.daily.metricflux.utils.NetworkUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimedLimiter implements RateLimiter, Stoppable {

    private final RateLimiter proxy;
    private final long timeoutMillis;
    private final FlinkMonitor monitor;
    private long startTS = 0;
    private boolean canceled = false;

    public TimedLimiter(
            RateLimiter proxy, long timeoutSeconds, String jobManagerHost, int jobManagerPort) {
        this.proxy = proxy;
        this.timeoutMillis = 1000 * timeoutSeconds;
        this.monitor = new FlinkMonitor(jobManagerHost, jobManagerPort);
    }

    @Override
    public long getDelayNanos(long count) {
        if (startTS == 0) {
            startTS = System.currentTimeMillis();
        }

        if (System.currentTimeMillis() - startTS > timeoutMillis) {
            if (!canceled) {
                monitor.cancelRunningJob();
                canceled = true;
            }
            // Finish fast
            return 0;
        }

        return proxy.getDelayNanos(count);
    }

    private static class FlinkMonitor {

        private final String jobManagerHost;
        private final int jobManagerPort;

        public FlinkMonitor(String jobManagerHost, int jobManagerPort) {
            this.jobManagerHost = jobManagerHost;
            this.jobManagerPort = jobManagerPort;
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
