package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class interacts with the {@link RayPolicyClient}, receiving actions (i.e. the next routing
 * table) and reporting observations (i.e. worker metrics). It also maintains the current episode ID
 * and step as understood by the Ray server.
 */
@Slf4j
@Deprecated
public class ExternalEnv implements Closeable {

    private static final int SKIP_STEPS = 5;
    private static final long UNINITIALIZED_ROUTE_VERSION = -1;

    private int step;
    private final int episodeLength;
    private final RLPolicyClient<double[], double[]> rlPolicyClient;
    private final MetricCollector metricCollector;
    private final ExecutorService es;
    private final RewardCalculator rewardCalculator;
    private long initialRouteVersion = UNINITIALIZED_ROUTE_VERSION;

    public ExternalEnv(
            String rayServerHost,
            int rayServerPort,
            int metricCollectorPort,
            int numWorkers,
            int episodeLength,
            double initialPerf)
            throws IOException {
        this(
                new RayPolicyClient<>(rayServerHost, rayServerPort),
                new MetricCollector(metricCollectorPort, numWorkers),
                episodeLength,
                initialPerf);
    }

    /** For testing. */
    public ExternalEnv(
            RLPolicyClient<double[], double[]> rlPolicyClient,
            MetricCollector metricCollector,
            int episodeLength,
            double initialPerf) {
        this.rlPolicyClient = rlPolicyClient;
        this.metricCollector = metricCollector;
        this.episodeLength = episodeLength;
        this.step = -1;
        this.es = Executors.newCachedThreadPool();
        this.es.submit(metricCollector);
        if (initialPerf == 0.0) {
            this.rewardCalculator = new RewardCalculator();
        } else {
            this.rewardCalculator = new RewardCalculator(initialPerf);
        }
    }

    @Override
    public void close() throws IOException {
        metricCollector.close();
        es.shutdownNow();
    }

    public CompletableFuture<NextActionResponse> getNextActionAsync(
            long currentRouteVersion, long requestStartTs) {

        if (initialRouteVersion == UNINITIALIZED_ROUTE_VERSION) {
            initialRouteVersion = currentRouteVersion;
        }

        if (currentRouteVersion - initialRouteVersion < SKIP_STEPS) {
            // Skip the first few steps to allow the Flink job to warm up
            return CompletableFuture.completedFuture(
                    new NextActionResponse(currentRouteVersion + 1, null, requestStartTs, null));
        }

        boolean done = false;
        if (step + 1 == episodeLength) {
            step = 0;
            done = true;
        } else {
            step++;
        }

        NextActionSupplier supplier =
                new NextActionSupplier(
                        currentRouteVersion,
                        requestStartTs,
                        done,
                        rewardCalculator,
                        metricCollector,
                        rlPolicyClient);
        return CompletableFuture.supplyAsync(supplier, es);
    }

    @AllArgsConstructor
    @Slf4j
    private static class NextActionSupplier implements Supplier<NextActionResponse> {

        private final long currentRouteVersion;
        private final long requestStartTs;
        private final boolean done;
        private final RewardCalculator rewardCalculator;
        private final MetricCollector metricCollector;
        private final RLPolicyClient<double[], double[]> rayPolicyClient;

        @Override
        public NextActionResponse get() {
            try {
                Map<Integer, double[]> metrics =
                        metricCollector.removeCompletedMetrics(currentRouteVersion);
                if (Objects.nonNull(metrics)) {
                    LOG.info("Received route {} metrics", currentRouteVersion);
                    double[] flatMetrics = flatten(metrics);
                    double[] nonIdleTimes = getNonIdleTimes(metrics);
                    double imbalance = getImbalance(nonIdleTimes);
                    double nmad = getNmad(nonIdleTimes);
                    Map<String, Object> info = getInfo(imbalance, nmad);
                    double reward = rewardCalculator.applyAsDouble(imbalance, nmad);
                    LOG.info(
                            "Route {} imbalance: {}, nmad: {}, reward: {}, non-idle times: {}",
                            currentRouteVersion,
                            imbalance,
                            nmad,
                            reward,
                            nonIdleTimes);
                    double[] obs = Arrays.stream(flatMetrics).map(d -> d / 1000).toArray();
                    double[] nextAction = rayPolicyClient.getNextAction(obs, reward, info, done);
                    return new NextActionResponse(
                            currentRouteVersion + 1, nextAction, requestStartTs, nonIdleTimes);
                } else {
                    LOG.error("Route {} metrics timeout", currentRouteVersion);
                    return new NextActionResponse(
                            currentRouteVersion + 1, null, requestStartTs, null);
                }
            } catch (Exception e) {
                LOG.error("Failed to get next action", e);
                return new NextActionResponse(currentRouteVersion + 1, null, requestStartTs, null);
            }
        }
    }

    @AllArgsConstructor
    @Getter
    public static class NextActionResponse {
        private final long nextRouteVersion;

        /** Next action. Null means passing action update. */
        private final double[] nextAction;

        private final long requestStartTs;
        private final int modelId;

        // metrics of the last window
        private final double[] metrics;

        public NextActionResponse(
                long nextRouteVersion, double[] nextAction, long requestStartTs, double[] metrics) {
            this(nextRouteVersion, nextAction, requestStartTs, 0, metrics);
        }
    }
}
