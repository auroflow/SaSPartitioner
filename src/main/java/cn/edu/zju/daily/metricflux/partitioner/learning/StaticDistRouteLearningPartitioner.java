package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.PENDING_UPDATE_ROUTE_VERSION;
import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.doubleArrayToString;
import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.intDoubleArrayMapToString;
import static cn.edu.zju.daily.metricflux.utils.HashUtils.hashPartition;
import static cn.edu.zju.daily.metricflux.utils.RoutingTableUtils.getRoutingTable;
import static smile.math.MathEx.sum;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.Partitioner;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Route learning for static data distribution.
 *
 * @param <R> record type
 */
@Slf4j
@Deprecated
public class StaticDistRouteLearningPartitioner<R extends Record<Integer>>
        extends Partitioner<Integer, R> {

    private final String rayServerHost;
    private final int rayServerPort;
    private final int metricCollectorPort;
    private final int numWorkers;
    private final int numHotKeys;
    private final int episodeLength;
    private final long initialRouteVersion;
    private final long routeUpdateIntervalMillis;
    private final double initialPerf;
    private final List<Pair<Integer, Integer>> mask;

    private ExternalEnv env;
    private AtomicReference<Map<Integer, double[]>> routingTable;
    private AtomicLong routeVersion;
    private AtomicLong lastRouteUpdateTS;
    private int[] hotCounts;

    /**
     * Constructor.
     *
     * @param rayServerHost The host address of the Ray server.
     * @param metricCollectorPort The port of the metric collector.
     * @param numWorkers The number of workers.
     * @param episodeLength The length of an episode.
     * @param mask The mask of the routing table, which is a list of pairs of (worker, key).
     */
    public StaticDistRouteLearningPartitioner(
            String rayServerHost,
            int rayServerPort,
            int metricCollectorPort,
            int numWorkers,
            int numHotKeys,
            int episodeLength,
            long initialRouteVersion,
            long routeUpdateIntervalMillis,
            double initialPerf,
            List<Pair<Integer, Integer>> mask) {

        if (initialRouteVersion < 0) {
            throw new IllegalArgumentException("Initial route version must be non-negative.");
        }

        this.rayServerHost = rayServerHost;
        this.rayServerPort = rayServerPort;
        this.metricCollectorPort = metricCollectorPort;
        this.numWorkers = numWorkers;
        this.numHotKeys = numHotKeys;
        this.episodeLength = episodeLength;
        this.initialRouteVersion = initialRouteVersion;
        this.routeUpdateIntervalMillis = routeUpdateIntervalMillis;
        this.mask = mask;
        this.initialPerf = initialPerf;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.env =
                new ExternalEnv(
                        rayServerHost,
                        rayServerPort,
                        metricCollectorPort,
                        numWorkers,
                        episodeLength,
                        initialPerf);
        this.routingTable = new AtomicReference<>(getInitialRoutingTable());
        this.routeVersion = new AtomicLong(initialRouteVersion);
        this.lastRouteUpdateTS = new AtomicLong(0L);
        this.hotCounts = new int[numWorkers];
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> out)
            throws Exception {

        updateRouteAsyncIfExpired(out);
        long rv = routeVersion.get();
        while (rv == PENDING_UPDATE_ROUTE_VERSION) {
            Thread.sleep(100);
            rv = routeVersion.get();
        }

        r.setRouteVersion(rv);
        int key = r.getKey();
        int worker = getWorker(key);
        if (worker == -1) {
            r.setHot(false);
            out.collect(new Tuple2<>(hashPartition(key, numWorkers), r));
        } else {
            hotCounts[worker]++;
            r.setHot(true);
            out.collect(new Tuple2<>(worker, r));
        }
    }

    private void updateRouteAsyncIfExpired(Collector<Tuple2<Integer, R>> out) {

        // initialize if not
        if (lastRouteUpdateTS.get() == 0) {
            lastRouteUpdateTS.set(System.currentTimeMillis());
        }

        long now = System.currentTimeMillis();
        long rv = routeVersion.get();
        if (now - lastRouteUpdateTS.get() > routeUpdateIntervalMillis
                && rv != PENDING_UPDATE_ROUTE_VERSION) {
            LOG.info("Route {} hot key summary: {}", rv, hotCounts);
            Arrays.fill(hotCounts, 0);
            LOG.info("Route {} expired. Requesting new route...", rv);

            for (int i = 0; i < numWorkers; i++) {
                // Send a null record to indicate the route version change.
                out.collect(Tuple2.of(i, null));
            }

            routeVersion.set(PENDING_UPDATE_ROUTE_VERSION);
            env.getNextActionAsync(rv, now)
                    .thenAccept(this::applyAction)
                    .exceptionally(
                            throwable -> {
                                LOG.error("Failed to get next action", throwable);
                                return null;
                            });
        }
    }

    private void applyAction(NextActionResponse response) {
        long nextRouteVersion = response.getNextRouteVersion();
        double[] nextAction = response.getNextAction();
        LOG.info(
                "Route {}: received next raw action: {}",
                nextRouteVersion,
                doubleArrayToString(nextAction, 3));
        long now = System.currentTimeMillis();
        routeVersion.set(nextRouteVersion);
        lastRouteUpdateTS.set(now);
        if (Objects.nonNull(nextAction)) {
            Map<Integer, double[]> nextRoutingTable =
                    getRoutingTable(mask, numWorkers, nextAction, numHotKeys);
            routingTable.set(nextRoutingTable);
            LOG.info(
                    "Route {} enforced, took {} ms, routing table:\n{}",
                    nextRouteVersion,
                    now - response.getRequestStartTs(),
                    intDoubleArrayMapToString(nextRoutingTable));
        } else {
            LOG.warn("Route {}: previous route kept due to null action", nextRouteVersion);
        }
    }

    /**
     * Sample a worker for the given key according to the routing table.
     *
     * @param key the key.
     * @return the worker, or -1 if the key is not found in the routing table (i.e. non-hot).
     */
    private int getWorker(int key) {
        Map<Integer, double[]> table = routingTable.get();

        double[] probs = table.get(key);
        if (Objects.isNull(probs)) {
            return -1;
        }

        // TODO: Use Alias method?
        double rand = Math.random();
        double sum = 0;
        for (int i = 0; i < probs.length; i++) {
            sum += probs[i];
            if (rand < sum) {
                return i;
            }
        }

        throw new IllegalStateException("Should not reach here.");
    }

    private Map<Integer, double[]> getInitialRoutingTable() {
        double[] action = new double[mask.size()];
        Arrays.fill(action, 1.0);
        return getRoutingTable(mask, numWorkers, action, numHotKeys);
    }

    @Override
    public String toString() {
        return "StaticDistRouteLearningPartitioner";
    }
}
