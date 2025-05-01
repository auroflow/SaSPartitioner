package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.*;
import static cn.edu.zju.daily.metricflux.utils.HashUtils.hashPartition;
import static smile.math.MathEx.sum;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.Partitioner;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.KeyWorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import cn.edu.zju.daily.metricflux.utils.RoutingTableUtils;
import cn.edu.zju.daily.metricflux.utils.ThreadUtils;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * RL-based partitioner that switches RL clients according to the current data distribution.
 *
 * @param <R> record type
 */
@Slf4j
public class SlidingRouteLearningPartitioner<R extends Record<Integer>>
        extends Partitioner<Integer, R> {

    private final String rayServerHost;
    private final int rayServerPort;
    private final int metricCollectorPort;
    private final int numWorkers;
    private final int numHotKeys;
    private final int numMetricsPerWorker;
    private final int episodeLength;
    private final int metricWindowNumSlides;
    private final double initialPref;
    private final List<Pair<Integer, Integer>> mask;

    private MetricCollector metricCollector;
    private RLPolicyClient<double[], double[]> policyClient;
    private AutoExternalEnv env;
    private AtomicReference<Map<Integer, double[]>> routingTable;
    private KeyWorkerStatistics<Integer> keyWorkerStatistics; // for hot keys
    private WorkerStatistics workerStatistics; // for all keys

    /**
     * Constructor.
     *
     * @param rayServerHost The host address of the Ray server.
     * @param metricCollectorPort The port of the metric collector.
     * @param numWorkers The number of workers.
     * @param episodeLength The length of an episode.
     * @param mask The mask of the routing table, which is a list of pairs of (worker, key).
     */
    public SlidingRouteLearningPartitioner(
            String rayServerHost,
            int rayServerPort,
            int metricCollectorPort,
            int numWorkers,
            int numHotKeys,
            int numMetricsPerWorker,
            int episodeLength,
            int metricWindowNumSlides,
            double initialPref,
            List<Pair<Integer, Integer>> mask) {

        this.rayServerHost = rayServerHost;
        this.rayServerPort = rayServerPort;
        this.metricCollectorPort = metricCollectorPort;
        this.numWorkers = numWorkers;
        this.numHotKeys = numHotKeys;
        this.numMetricsPerWorker = numMetricsPerWorker;
        this.episodeLength = episodeLength;
        this.metricWindowNumSlides = metricWindowNumSlides;
        this.initialPref = initialPref;
        this.mask = mask;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.metricCollector = new MetricCollector(metricCollectorPort, numWorkers);
        this.keyWorkerStatistics = new KeyWorkerStatistics<>(metricWindowNumSlides, numWorkers);
        this.workerStatistics = new WorkerStatistics(metricWindowNumSlides);
        this.policyClient = new RayPolicyClient<>(rayServerHost, rayServerPort);
        this.env =
                new AutoExternalEnv(
                        metricWindowNumSlides,
                        episodeLength,
                        numWorkers,
                        numMetricsPerWorker,
                        initialPref,
                        policyClient,
                        metricCollector,
                        workerStatistics,
                        this::applyAction);
        this.routingTable = new AtomicReference<>(getInitialRoutingTable());
        Thread envThread = new Thread(this.env);
        envThread.setUncaughtExceptionHandler(ThreadUtils.getUncaughtExceptionHandler());
        envThread.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.env.close();
        this.policyClient.close();
        this.metricCollector.close();
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> out)
            throws Exception {

        int key = r.getKey();
        int worker = getWorker(key);
        if (worker == -1) {
            r.setHot(false);
            worker = hashPartition(key, numWorkers);
            out.collect(new Tuple2<>(worker, r));
        } else {
            r.setHot(true);
            keyWorkerStatistics.add(key, worker);
            out.collect(new Tuple2<>(worker, r));
        }
        workerStatistics.add(worker);
    }

    private void applyAction(NextActionResponse response) {
        // null response means the initial window has not completed
        if (Objects.nonNull(response)) {
            double[] nextAction = response.getNextAction();
            LOG.debug("Received next raw action: {}", doubleArrayToString(nextAction, 3));
            long now = System.currentTimeMillis();
            if (Objects.nonNull(nextAction)) {
                Map<Integer, double[]> nextRoutingTable =
                        RoutingTableUtils.getRoutingTable(mask, numWorkers, nextAction, numHotKeys);
                LOG.info("New route enforced (took {} ms)", now - response.getRequestStartTs());
                // :\nWorker statistics of previous slide:\n{}\nActual routing of previous
                // slide:\n{}\nRouting table of new slide:\n{}
                // intIntArrayMapToString(keyWorkerStatistics.getWorkerCounts(false)),
                // intDoubleArrayMapToString(keyWorkerStatistics.getWorkerRatios(false)),
                // intDoubleArrayMapToString(nextRoutingTable)
                routingTable.set(nextRoutingTable);
            } else {
                LOG.warn("Previous route kept due to null action");
            }
        }

        keyWorkerStatistics.newSlide();
        workerStatistics.newSlide();
    }

    /**
     * Sample a worker for the given key according to the routing table.
     *
     * @param key the key.
     * @return the worker, or -1 if the key is not found in the routing table (i.e. non-hot).
     */
    private int getWorker(int key) {
        double[] probs = routingTable.get().get(key);

        if (Objects.isNull(probs)) {
            return -1;
        }

        int[] current = keyWorkerStatistics.getWorkerCount(key, false);
        long sum = sum(current);

        int worker = -1;
        double priority = -Double.MAX_VALUE;
        for (int i = 0; i < numWorkers; i++) {
            if (probs[i] == 0) {
                continue;
            }
            double ratio = sum == 0 ? 0 : ((double) current[i] / sum);
            double p = probs[i] - ratio;
            if (p > priority) {
                worker = i;
                priority = p;
            }
        }

        return worker;
    }

    private Map<Integer, double[]> getInitialRoutingTable() {
        double[] action = new double[mask.size()];
        Arrays.fill(action, 1.0);
        return RoutingTableUtils.getRoutingTable(mask, numWorkers, action, numHotKeys);
    }

    @Override
    public String toString() {
        return "SlidingRouteLearningPartitioner";
    }
}
