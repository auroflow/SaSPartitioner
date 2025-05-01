package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.METRIC_REPORT_MAGIC_NUMBER;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for collecting metrics from the combiners, and sending complete metrics of each route
 * metricWindow to the {@link ExternalEnv}.
 *
 * <p>This class can be used as a Runnable, or by calling remove(Any)CompletedMetrics manually.
 */
public class MetricCollector implements Runnable, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricCollector.class);

    private static final long SLEEP_INTERVAL_MILLIS = 250;
    private static final long METRIC_COLLECT_TIMEOUT_MILLIS = 60000;
    // If a worker times out, use this value as the metric
    private static final double[] MISSING_METRIC = new double[] {2000.0};
    private final int metricCollectorPort;
    private final int numWorkers;

    private final Map<Long, Map<Integer, double[]>> idsToMetrics = new HashMap<>();
    private final Set<Long> finishedMetricIds = new HashSet<>();

    private final ExecutorService executor;
    private ServerSocket metricCollectorServer;

    private volatile boolean stopped = false;

    public MetricCollector(int metricCollectorPort, int numWorkers) {
        this.metricCollectorPort = metricCollectorPort;
        this.numWorkers = numWorkers;
        this.executor = Executors.newCachedThreadPool();
    }

    /** Start the metric collector as a runnable. */
    @Override
    public void run() {
        try {
            metricCollectorServer = new ServerSocket(metricCollectorPort, 100);
            LOG.info("Metric report server started on port {}", metricCollectorPort);
            while (!stopped) {
                Socket incoming = metricCollectorServer.accept();
                executor.submit(new MetricReportHandler(incoming));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() throws IOException {
        metricCollectorServer.close();
        executor.shutdownNow();
        stopped = true;
    }

    /** This method is not thoroughly tested. */
    @Override
    public void close() throws IOException {
        stop();
    }

    private synchronized void addMetrics(MetricReportMessage message) {
        if (finishedMetricIds.contains(message.getMetricId())) {
            LOG.debug(
                    "Route {}: received from subtask {} for finished route",
                    message.getMetricId(),
                    message.getWorker());
            return;
        }
        double[] metrics = message.getMetrics();
        // the last item is the busy time. 1000 means the worker is very likely back-pressured, so
        // we penalize it.
        //        if (metrics[metrics.length - 1] == 1000.0) {
        //            metrics[metrics.length - 1] = 2000.0;
        //        }
        Map<Integer, double[]> subtasks =
                idsToMetrics.computeIfAbsent(message.getMetricId(), k -> new HashMap<>());
        subtasks.put(message.getWorker(), message.getMetrics());
        if (subtasks.size() == numWorkers) {
            notifyAll();
        }
    }

    public synchronized Pair<Long, Map<Integer, double[]>> removeAnyCompletedMetrics() {
        while (!stopped) {

            List<Long> ids = new ArrayList<>(idsToMetrics.keySet());
            ids.sort(Comparator.comparingLong(Long::longValue).reversed());

            for (long metricId : ids) {
                if (finishedMetricIds.contains(metricId)) {
                    removeCompletedMetricsSmallerThan(metricId);
                    break;
                }

                if (idsToMetrics.get(metricId).size() == numWorkers) {
                    Map<Integer, double[]> metrics = idsToMetrics.remove(metricId);
                    finishedMetricIds.add(metricId);
                    removeCompletedMetricsSmallerThan(metricId);
                    return Pair.of(metricId, metrics);
                }
            }

            try {
                wait();
            } catch (InterruptedException ignored) {
            }
        }
        return null;
    }

    private void removeCompletedMetricsSmallerThan(long targetId) {
        List<Long> ids = new ArrayList<>(idsToMetrics.keySet());
        for (long metricId : ids) {
            if (metricId < targetId && idsToMetrics.get(metricId).size() == numWorkers) {
                idsToMetrics.remove(metricId);
                finishedMetricIds.add(metricId);
            }
        }
    }

    /**
     * Wait for and return the metrics of a route version until all workers have reported their
     * metrics. Return null if timeout (and the route will be unchanged).
     *
     * @param metricId The route version
     * @return The metrics, or null if timeout
     */
    public synchronized Map<Integer, double[]> removeCompletedMetrics(long metricId) {
        Map<Integer, double[]> metrics = idsToMetrics.get(metricId);
        long waitStart = System.currentTimeMillis();
        while (Objects.isNull(metrics) || metrics.size() < numWorkers) {
            long waited = System.currentTimeMillis() - waitStart;
            if (waited > 0) {
                LOG.info(
                        "Route {}: metrics waited {} ms, {} workers collected",
                        metricId,
                        waited,
                        Objects.isNull(metrics) ? 0 : metrics.size());
            }
            if (waited >= METRIC_COLLECT_TIMEOUT_MILLIS) {
                break;
            }
            try {
                LOG.debug(
                        "Route {}: will wait for at most {} ms",
                        metricId,
                        METRIC_COLLECT_TIMEOUT_MILLIS - waited);
                wait(Math.min(SLEEP_INTERVAL_MILLIS, METRIC_COLLECT_TIMEOUT_MILLIS - waited));
            } catch (InterruptedException ignored) {
            }
            metrics = idsToMetrics.get(metricId);
        }

        // Metrics are complete, or timeout

        long now = System.currentTimeMillis();
        if (Objects.nonNull(metrics) && metrics.size() == numWorkers) {
            LOG.info("Route {}: metric collect successful in {} ms", metricId, now - waitStart);
        } else {
            // Usually this means some workers are back-pressured. We assign them with predefined
            // metrics that indicate back-pressure.
            List<Integer> missing = new ArrayList<>();
            for (int i = 0; i < numWorkers; i++) {
                if (!metrics.containsKey(i)) {
                    missing.add(i);
                    metrics.put(i, MISSING_METRIC);
                }
            }

            LOG.warn(
                    "Route {}: Metric collect timeout for workers {} in {} ms, using missing values",
                    metricId,
                    missing,
                    now - waitStart);
        }
        metrics = idsToMetrics.remove(metricId);
        finishedMetricIds.add(metricId);
        return metrics;
    }

    private class MetricReportHandler implements Runnable {

        private final Socket incoming;

        public MetricReportHandler(Socket incoming) {
            this.incoming = incoming;
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(incoming.getInputStream())) {
                LOG.info(
                        "Connected to {}:{}",
                        incoming.getRemoteSocketAddress(),
                        incoming.getPort());

                // Authentication
                int magicNumber = in.readInt();
                if (magicNumber != METRIC_REPORT_MAGIC_NUMBER) {
                    LOG.error("Invalid magic number: {}", magicNumber);
                    return;
                }

                while (!stopped) {
                    MetricReportMessage message = (MetricReportMessage) in.readObject();
                    if (message == null) {
                        break;
                    }
                    // process the message
                    addMetrics(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
