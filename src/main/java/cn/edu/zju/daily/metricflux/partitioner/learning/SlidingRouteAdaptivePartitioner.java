package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.*;
import static cn.edu.zju.daily.metricflux.utils.HashUtils.hashPartition;
import static smile.math.MathEx.sum;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.Partitioner;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.KeyWorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import cn.edu.zju.daily.metricflux.utils.DistributionUtils;
import cn.edu.zju.daily.metricflux.utils.RoutingTableUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
public class SlidingRouteAdaptivePartitioner<R extends Record<Integer>>
        extends Partitioner<Integer, R> {

    // These four arrays have the same length. Each dim represents a distribution
    private final List<String> rayServerHosts;
    private final List<Integer> rayServerPorts;
    private final List<List<Pair<Integer, Integer>>> masks;
    private final List<double[]> dists;

    private final AtomicReference<List<Integer>> currentHotKeys;

    private final int metricCollectorPort;
    private final int numWorkers;
    private final int numHotKeys;
    private final int numMetricsPerWorker;
    private final int episodeLength;
    private final int metricWindowNumSlides;
    private final double initialPerf;
    private long count = 0;

    private MetricCollector metricCollector;
    private List<RLPolicyClient<double[], double[]>> policyClients;
    private AdaptiveAutoExternalEnv env;
    private AtomicReference<Map<Integer, double[]>> routingTable;
    private KeyWorkerStatistics<Integer> keyWorkerStatistics; // for hot keys
    private WorkerStatistics workerStatistics; // for all keys
    private Map<Integer, Integer> keyStatistics; // for all keys

    /**
     * Constructor.
     *
     * @param rayServerHosts The host address of the Ray server.
     * @param rayServerPorts
     * @param metricCollectorPort The port of the metric collector.
     * @param numWorkers The number of workers.
     * @param episodeLength The length of an episode.
     * @param masks The mask of the routing table, which is a list of pairs of (worker, key).
     */
    public SlidingRouteAdaptivePartitioner(
            List<String> rayServerHosts,
            List<Integer> rayServerPorts,
            List<List<Pair<Integer, Integer>>> masks,
            List<double[]> dists,
            int metricCollectorPort,
            int numWorkers,
            int numHotKeys,
            int numMetricsPerWorker,
            int episodeLength,
            int metricWindowNumSlides,
            double initialPerf) {

        this.rayServerHosts = rayServerHosts;
        this.rayServerPorts = rayServerPorts;
        this.masks = masks;
        this.dists = dists;

        this.currentHotKeys = new AtomicReference<>(Collections.emptyList());

        this.metricCollectorPort = metricCollectorPort;
        this.numWorkers = numWorkers;
        this.numHotKeys = numHotKeys;
        this.numMetricsPerWorker = numMetricsPerWorker;
        this.episodeLength = episodeLength;
        this.metricWindowNumSlides = metricWindowNumSlides;
        this.initialPerf = initialPerf;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.metricCollector = new MetricCollector(metricCollectorPort, numWorkers);
        this.keyWorkerStatistics = new KeyWorkerStatistics<>(metricWindowNumSlides, numWorkers);
        this.workerStatistics = new WorkerStatistics(metricWindowNumSlides);
        this.keyStatistics = new ConcurrentHashMap<>();
        this.policyClients = new ArrayList<>();
        for (int i = 0; i < rayServerHosts.size(); i++) {
            if (!"none".equalsIgnoreCase(rayServerHosts.get(i))) {
                this.policyClients.add(
                        new RayPolicyClient<>(rayServerHosts.get(i), rayServerPorts.get(i)));
            } else {
                this.policyClients.add(null);
            }
        }
        this.env =
                new AdaptiveAutoExternalEnv(
                        metricWindowNumSlides,
                        episodeLength,
                        numWorkers,
                        numMetricsPerWorker,
                        initialPerf,
                        policyClients,
                        metricCollector,
                        workerStatistics,
                        this::applyAction);
        this.routingTable = new AtomicReference<>(getInitialRoutingTable());
        new Thread(this.env).start();
        for (int i = 0; i < rayServerHosts.size(); i++) {
            LOG.info(
                    "Model {}: {}:{}, masks = {}, dists = {}",
                    i,
                    rayServerHosts.get(i),
                    rayServerPorts.get(i),
                    masks.get(i),
                    dists.get(i));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.env.close();
        for (RLPolicyClient<double[], double[]> client : policyClients) {
            client.close();
        }
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
        keyStatistics.put(key, keyStatistics.getOrDefault(key, 0) + 1);
        count++;
    }

    private void applyAction(NextActionResponse response) {
        // null response means the initial window has not completed
        if (Objects.nonNull(response)) {
            double[] nextAction = response.getNextAction();
            LOG.info("Received next raw action: {}", doubleArrayToString(nextAction, 3));
            long now = System.currentTimeMillis();
            int model = response.getModelId();
            if (Objects.nonNull(nextAction)) {
                Map<Integer, double[]> nextRoutingTable =
                        RoutingTableUtils.getRoutingTable(
                                masks.get(model),
                                numWorkers,
                                nextAction,
                                currentHotKeys.get(),
                                numHotKeys);
                LOG.info("count: {}", count);
                LOG.info(
                        "New route enforced (took {} ms):\nWorker statistics of previous slide:\n{}\nActual routing of previous slide:\n{}\nRouting table of new slide:\n{}",
                        now - response.getRequestStartTs(),
                        intIntArrayMapToString(keyWorkerStatistics.getWorkerCounts(false)),
                        intDoubleArrayMapToString(keyWorkerStatistics.getWorkerRatios(false)),
                        intDoubleArrayMapToString(nextRoutingTable));
                routingTable.set(nextRoutingTable);
            } else {
                LOG.warn("Previous route kept due to null action");
            }
        }

        keyWorkerStatistics.newSlide();
        workerStatistics.newSlide();

        selectNextModel();
    }

    private Pair<List<Integer>, double[]> getHotKeysWithDistribution(int numHotKeys) {
        // Get the hottest keys
        Map<Integer, Integer> snapshot = Map.copyOf(keyStatistics);
        List<Integer> keys = new ArrayList<>(snapshot.keySet());
        keys.sort(Comparator.comparingInt(snapshot::get).reversed());
        List<Integer> hotKeys = keys.subList(0, Math.min(numHotKeys, keys.size()));
        double[] dist = new double[numHotKeys];
        for (int i = 0; i < numHotKeys; i++) {
            if (i < hotKeys.size()) {
                dist[i] = keyStatistics.get(hotKeys.get(i));
            }
        }
        double sum = sum(dist);
        for (int i = 0; i < dist.length; i++) {
            if (sum > 0) {
                dist[i] /= sum;
            }
        }
        return Pair.of(hotKeys, dist);
    }

    private void selectNextModel() {

        Pair<List<Integer>, double[]> pair = getHotKeysWithDistribution(numHotKeys);
        // Select the most similar distribution
        int model = 0;
        double bestDist = DistributionUtils.euclidian(pair.getRight(), dists.get(0));
        for (int i = 1; i < dists.size(); i++) {
            double dist = DistributionUtils.euclidian(pair.getRight(), dists.get(i));
            if (dist < bestDist) {
                model = i;
                bestDist = dist;
            }
        }

        this.currentHotKeys.set(pair.getLeft());
        boolean changed = this.env.setCurrentModel(model);

        LOG.info(
                "Switching model to {}, current hot keys: {}, dist: {}",
                model,
                pair.getLeft(),
                pair.getRight());

        keyStatistics.clear();
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
        return RoutingTableUtils.getRoutingTable(null, numWorkers, new double[0], numHotKeys);
    }

    @Override
    public String toString() {
        return "SlidingRouteLearningPartitioner";
    }
}
