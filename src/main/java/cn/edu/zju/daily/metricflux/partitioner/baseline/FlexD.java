package cn.edu.zju.daily.metricflux.partitioner.baseline;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.partitioner.container.HotStatistics;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class FlexD<R extends Record<Integer>> extends MetricPartitioner<R> {

    private static final double HOT_THRESHOLD = 0.01;

    private HotStatistics hotStat; // hot key detector
    private WorkerStatistics workerStat;
    private Map<Integer, Double> freqMappingTable;
    private Map<Integer, Set<Integer>> workerMappingTable;

    private final int parallelism;
    private final int numKeys;
    private List<Integer> allWorkers;

    private static final long REFRESH_FREQUENCY = 60_000L;
    private static final double DELTA = 1.1;
    private long nextRefreshTs = 0;

    public FlexD(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            int slide, // milliseconds
            int size, // milliseconds
            int numOfKeys,
            double initialPerf) {
        super(
                metricCollectorPort,
                numWorkers,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);
        parallelism = numWorkers;
        numKeys = numOfKeys;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        allWorkers = IntStream.range(0, parallelism).boxed().collect(toList());
        hotStat = new HotStatistics(parallelism, numKeys, REFRESH_FREQUENCY);
        workerStat = new WorkerStatistics(2);
        freqMappingTable = new HashMap<>();
        workerMappingTable = new HashMap<>();
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> collector)
            throws Exception {

        int key = r.getKey();
        int worker = -1;
        boolean isHot;
        double freq = hotStat.getFrequency(key);
        if (freq < HOT_THRESHOLD) {
            isHot = false;
            worker = partitionCold(key);
            LOG.trace("Key {} is cold with freq {}, assigned to worker {}", key, freq, worker);
        } else {
            isHot = true;
            if (freqMappingTable.containsKey(key)) {
                assert workerMappingTable.containsKey(key);
                Set<Integer> mappedWorkers = workerMappingTable.get(key);
                if (freq > freqMappingTable.get(key)) {
                    // increase in frequency, add a worker
                    worker = getWorkerWithLightestLoad(allWorkers, mappedWorkers);
                    freqMappingTable.put(key, freqMappingTable.get(key) * DELTA);
                    mappedWorkers.add(worker);
                } else {
                    worker = getWorkerWithLightestLoad(mappedWorkers, null);
                }
                LOG.trace(
                        "Key {} is hot with freq {}, assigned to worker {}, mapped workers: {}",
                        key,
                        freq,
                        worker,
                        mappedWorkers);
            } else {
                // first time seeing this hot key
                worker = getWorkerWithLightestLoad();
                freqMappingTable.put(key, freq);
                Set<Integer> mappedWorkers = new HashSet<>();
                mappedWorkers.add(worker);
                workerMappingTable.put(key, mappedWorkers);

                LOG.trace(
                        "Key {} is hot with freq {}, assigned to worker {}, first time",
                        key,
                        freq,
                        worker);
            }
        }

        long currentTs = r.getTs();
        if (currentTs >= nextRefreshTs) {
            nextRefreshTs = currentTs + REFRESH_FREQUENCY;
            LOG.info("Refreshing Flex-D state at {}", currentTs);
            LOG.info("Current freq table size: {}", freqMappingTable.size());
            LOG.info("Current worker mapping table size: {}", workerMappingTable.size());
            LOG.info("Current worker statistics: {}", workerStat.getWorkerCounts(parallelism));
            freqMappingTable.clear();
            workerMappingTable.clear();
            // hotStat manages the refreshing of its own state
            workerStat.newSlide();
        }

        applyEffect(key, worker, isHot);
        hotStat.isHot(r, numKeys, null);
        workerStat.add(worker);
        collector.collect(new Tuple2<>(worker, r));
    }

    private int partitionCold(int key) {
        return key % parallelism;
    }

    private int getWorkerWithLightestLoad() {
        return getWorkerWithLightestLoad(allWorkers, null);
    }

    private int getWorkerWithLightestLoad(
            Collection<Integer> candidates, Collection<Integer> exclude) {
        assert !candidates.isEmpty();
        int bestWorker = -1;
        double bestLoad = Double.MAX_VALUE;
        for (int worker : candidates) {
            if (exclude != null && exclude.contains(worker)) {
                continue;
            }
            int count = workerStat.getCount(worker);
            double load = count == 0 ? 0.0 : (double) count / workerStat.getTotalCount();
            if (load < bestLoad) {
                bestWorker = worker;
                bestLoad = load;
            }
        }

        if (bestWorker == -1) {
            return getWorkerWithLightestLoad(candidates, null);
        }

        return bestWorker;
    }
}
