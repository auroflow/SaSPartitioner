package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.*;

import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import cn.edu.zju.daily.metricflux.utils.ArrayUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/** Connect to different ray clients according to data count. */
@Slf4j
public class AdaptiveAutoExternalEnv implements Runnable, Closeable {

    private final int numWorkers;
    private final MetricCollector metricCollector;
    private final List<RLPolicyClient<double[], double[]>> rlPolicyClients;
    private final int metricWindowSize;
    private final Consumer<NextActionResponse> actionCallback;
    private final SlidingWindow metricWindow;
    private final SlidingWindow nonIdleTimeWindow;
    private final RewardCalculator rewardCalculator;
    private final WorkerStatistics statistics;
    private final int episodeLength;
    private final AtomicInteger currentModel;
    private int step;
    private int receivedCount;

    private boolean stopped;

    public AdaptiveAutoExternalEnv(
            int metricWindowSize,
            int episodeLength,
            int numWorkers,
            int metricsPerWorker,
            double initialPref,
            List<RLPolicyClient<double[], double[]>> policyClients,
            MetricCollector metricCollector,
            WorkerStatistics statistics,
            Consumer<NextActionResponse> actionCallback)
            throws IOException {
        this.numWorkers = numWorkers;
        this.metricWindowSize = metricWindowSize;
        this.metricWindow = new SlidingWindow(metricWindowSize, numWorkers * metricsPerWorker);
        this.episodeLength = episodeLength;
        this.nonIdleTimeWindow = new SlidingWindow(metricWindowSize, numWorkers);
        this.rlPolicyClients = policyClients;
        this.metricCollector = metricCollector;
        this.statistics = statistics;
        this.actionCallback = actionCallback;
        this.rewardCalculator = new RewardCalculator(initialPref);
        this.currentModel = new AtomicInteger(0);
    }

    public boolean setCurrentModel(int model) {
        int oldModel = this.currentModel.getAndSet(model);
        return oldModel != model;
    }

    public int getCurrentModel() {
        return this.currentModel.get();
    }

    @Override
    public void run() {

        new Thread(this.metricCollector).start();

        while (!stopped) {
            Pair<Long, Map<Integer, double[]>> idsAndMetrics =
                    metricCollector.removeAnyCompletedMetrics();

            if (Objects.isNull(idsAndMetrics)) {
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
                actionCallback.accept(null);
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
                    "Metric id {} imbalance: {}, nmad: {}, reward: {}, non-idle times: {}",
                    metricWindowStartTs,
                    imbalance,
                    nmad,
                    reward,
                    ArrayUtils.doubleArrayToString(meanNonIdleTimes, 3));

            NextActionResponse response = null;
            int model = currentModel.get();
            RLPolicyClient<double[], double[]> rlPolicyClient = rlPolicyClients.get(model);
            if (Objects.nonNull(rlPolicyClient)) {
                long start = System.currentTimeMillis();
                double[] nextAction = rlPolicyClient.getNextAction(obs, reward, info, done);
                LOG.info("Time to get next action: {} ms", System.currentTimeMillis() - start);
                response =
                        new NextActionResponse(
                                0, nextAction, metricWindowStartTs, model, meanNonIdleTimes);
            } else {
                // Used for even distribution
                response =
                        new NextActionResponse(
                                0, new double[0], metricWindowStartTs, model, meanNonIdleTimes);
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
