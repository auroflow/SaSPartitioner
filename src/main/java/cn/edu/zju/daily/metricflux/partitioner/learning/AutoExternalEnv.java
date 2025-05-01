package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.*;

import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import cn.edu.zju.daily.metricflux.utils.ArrayUtils;
import cn.edu.zju.daily.metricflux.utils.ThreadUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Different from {@link ExternalEnv}:
 *
 * <ol>
 *   <li>This class is a Runnable, which continuously monitors metric reports. If a metric
 *       metricWindow has completed (i.e. metrics received from every worker in that metricWindow),
 *       it sends to observations to the Ray server.
 *   <li>Receives a callback function which is used to update the routing table of the partitioner.
 *   <li>Reports average metrics across a metric window.
 *   <li>Includes worker statistics as observations.
 * </ol>
 */
@Slf4j
public class AutoExternalEnv implements Runnable, Closeable {

    private final int numWorkers;
    private final MetricCollector metricCollector;
    private final RLPolicyClient<double[], double[]> rlPolicyClient;
    private final int metricWindowSize;
    private final Consumer<NextActionResponse> actionCallback;
    private final SlidingWindow metricWindow;
    private final SlidingWindow nonIdleTimeWindow;
    private final RewardCalculator rewardCalculator;
    private final WorkerStatistics statistics;
    private final int episodeLength;
    private int step;
    private int receivedCount;

    private boolean stopped;

    public AutoExternalEnv(
            int metricWindowSize,
            int episodeLength,
            int numWorkers,
            int metricsPerWorker,
            double initialPref,
            RLPolicyClient<double[], double[]> policyClient,
            MetricCollector metricCollector,
            WorkerStatistics statistics,
            Consumer<NextActionResponse> actionCallback)
            throws IOException {
        this.numWorkers = numWorkers;
        this.metricWindowSize = metricWindowSize;
        this.metricWindow = new SlidingWindow(metricWindowSize, numWorkers * metricsPerWorker);
        this.episodeLength = episodeLength;
        this.nonIdleTimeWindow = new SlidingWindow(metricWindowSize, numWorkers);
        this.rlPolicyClient = policyClient;
        this.metricCollector = metricCollector;
        this.statistics = statistics;
        this.actionCallback = actionCallback;
        this.rewardCalculator = new RewardCalculator(initialPref);
    }

    /**
     * Creates an AutoExternalEnv that only collects metrics periodically and that does not use a
     * Ray policy.
     */
    public AutoExternalEnv(
            int metricWindowSize,
            int episodeLength,
            int numWorkers,
            int metricsPerWorker,
            double initialPref,
            MetricCollector metricCollector,
            WorkerStatistics statistics,
            Consumer<NextActionResponse> actionCallback)
            throws IOException {
        this(
                metricWindowSize,
                episodeLength,
                numWorkers,
                metricsPerWorker,
                initialPref,
                null,
                metricCollector,
                statistics,
                actionCallback);
    }

    @Override
    public void run() {

        Thread metricCollectorThread = new Thread(this.metricCollector);
        metricCollectorThread.setUncaughtExceptionHandler(
                ThreadUtils.getUncaughtExceptionHandler());
        metricCollectorThread.start();
        LOG.info("Started metric collector thread.");

        while (!stopped) {
            Pair<Long, Map<Integer, double[]>> idsAndMetrics =
                    metricCollector.removeAnyCompletedMetrics();

            if (Objects.isNull(idsAndMetrics)) {
                LOG.info("No completed metrics yet.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }

            long metricWindowStartTs = idsAndMetrics.getLeft();
            Map<Integer, double[]> metrics = idsAndMetrics.getRight();
            LOG.info("Received metrics {}", metricWindowStartTs);
            double[] flatMetrics = flatten(metrics);
            metricWindow.add(flatMetrics);
            double[] meanFlatMetrics = metricWindow.value(); // obs part 1

            double[] nonIdleTimes = getNonIdleTimes(metrics);
            nonIdleTimeWindow.add(nonIdleTimes);
            double[] meanNonIdleTimes = nonIdleTimeWindow.value(); // for reward

            if (receivedCount < metricWindowSize) {
                receivedCount++;
                // indicates that a window has not completed
                if (Objects.nonNull(actionCallback)) {
                    actionCallback.accept(null);
                }
                continue;
            }
            double imbalance = getImbalance(meanNonIdleTimes); // reward part 1
            double nmad = getNmad(meanNonIdleTimes); // reward part 2
            Map<String, Object> info = getInfo(imbalance, nmad);
            double reward = rewardCalculator.applyAsDouble(imbalance, nmad);

            int[] workerCounts = statistics.getWorkerCounts(numWorkers);
            double[] normalized = ArrayUtils.normalize(workerCounts);
            ArrayUtils.crop(normalized, 0, 2); // obs part 2

            double[] obs =
                    ArrayUtils.concat(
                            normalized,
                            Arrays.stream(meanFlatMetrics).map(d -> d / 1000).toArray());

            // done?
            step++;
            boolean done = false;
            if (step == episodeLength) {
                done = true;
                step = 0;
            }

            LOG.info(
                    "Metric id {} imbalance: {}, nmad: {}, reward: {}",
                    metricWindowStartTs,
                    imbalance,
                    nmad,
                    reward);
            LOG.info("non-idle times: {}", ArrayUtils.doubleArrayToString(meanNonIdleTimes, 3));
            LOG.info("Worker counts: {}", workerCounts);
            LOG.info("Obs: {}", ArrayUtils.doubleArrayToString(obs, 3));

            NextActionResponse response = null;
            if (Objects.nonNull(rlPolicyClient)) {
                long start = System.currentTimeMillis();
                double[] nextAction = rlPolicyClient.getNextAction(obs, reward, info, done);
                LOG.info("Time to get next action: {} ms", System.currentTimeMillis() - start);
                response =
                        new NextActionResponse(
                                0, nextAction, metricWindowStartTs, meanNonIdleTimes);
            } else {
                response = new NextActionResponse(0, null, metricWindowStartTs, meanNonIdleTimes);
            }
            if (Objects.nonNull(actionCallback)) {
                actionCallback.accept(response);
            }
        }
    }

    @Override
    public void close() throws IOException {
        stopped = true;
    }

    private static class SlidingWindow {

        int windowSize;
        int length;
        double[][] values;
        boolean full;
        int currentIndex;

        public SlidingWindow(int windowSize, int length) {
            this.windowSize = windowSize;
            this.length = length;
            this.values = new double[windowSize][];
        }

        public void add(double[] value) {
            values[currentIndex] = value;
            currentIndex++;
            if (currentIndex == windowSize) {
                currentIndex = 0;
                full = true;
            }
        }

        public double[] value() {
            int start = 0;
            int end = full ? windowSize : currentIndex;
            if (start >= end) {
                return new double[length];
            }

            double[] mean = new double[length];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < length; j++) {
                    mean[j] += values[i][j] / (end - start);
                }
            }
            return mean;
        }
    }
}
