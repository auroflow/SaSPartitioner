package cn.edu.zju.daily.metricflux.task.wordcount.operator;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.*;

import cn.edu.zju.daily.metricflux.partitioner.learning.MetricCollector;
import cn.edu.zju.daily.metricflux.partitioner.learning.MetricReportMessage;
import cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils;
import cn.edu.zju.daily.metricflux.partitioner.metrics.monitor.JobInfo;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A combiner that sends metrics to the {@link MetricCollector} when the route version changes to
 * {@link RemoteLearningUtils#PENDING_UPDATE_ROUTE_VERSION}
 *
 * <p>Message format:
 *
 * <ul>
 *   <li>Connection establishment: {@link RemoteLearningUtils#METRIC_REPORT_MAGIC_NUMBER}
 *   <li>Metric report: {@link MetricReportMessage}
 * </ul>
 */
public class RouteLearningWordCountCombiner extends WordCountCombiner {

    private static final Logger LOG = LoggerFactory.getLogger(RouteLearningWordCountCombiner.class);

    /**
     * The name of a task which runs on the same host as the metric collector (in this case, the
     * partitioner).
     */
    private static final String METRIC_COLLECTOR_TASK_NAME = "partitioner";

    private final String jobManagerHost;
    private final int jobManagerPort;
    private final int metricCollectorPort;
    private Socket metricCollectorSocket;
    private ObjectOutputStream out;

    public RouteLearningWordCountCombiner(
            int workloadRatio, String jobManagerHost, int jobManagerPort, int metricCollectorPort) {
        super(workloadRatio);
        this.jobManagerHost = jobManagerHost;
        this.jobManagerPort = jobManagerPort;
        this.metricCollectorPort = metricCollectorPort;
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

    @Override
    protected void reportMetrics(
            long routeVersion, int subtaskIndex, double[] metrics, long routeDuration) {
        // send metrics to the reporter
        if (out == null) {
            LOG.error(
                    "Failed to send metrics {}:{} to the reporter: reporter socket not initialized.",
                    routeVersion,
                    subtaskIndex);
            return;
        }
        try {
            out.writeObject(
                    new MetricReportMessage(routeVersion, subtaskIndex, metrics, routeDuration));
            out.flush();
        } catch (IOException e) {
            LOG.error(
                    "Failed to send metrics {}:{} to the reporter: IO error",
                    routeVersion,
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
