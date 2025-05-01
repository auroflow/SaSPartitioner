package cn.edu.zju.daily.metricflux.partitioner.learning;

import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.getImbalance;
import static cn.edu.zju.daily.metricflux.partitioner.learning.RemoteLearningUtils.getNonIdleTimes;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.Partitioner;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.KeyWorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.StatRewardAssigner;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.StaticHotKeyCBandit;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class SlidingRouteDaltonStatPartitioner<R extends Record<Integer>>
        extends Partitioner<Integer, R> {

    private final int combinerParallelism;
    private final int numMetricsPerWorker;
    private final int metricNumSlides;
    private final double a;
    private final double epsilon;
    private final double temperature;
    private final double initialPerf;
    private final Map<Integer, List<Integer>> trueHotKeysToWorkers;
    private final int numHotKeys;
    private KeyWorkerStatistics<Integer> keyWorkerStatistics; // for hot keys
    private WorkerStatistics coldWorkerStatistics;

    private StaticHotKeyCBandit<R> cbandit;

    private MetricCollector metricCollector;
    private AutoExternalEnv env;
    private final int metricCollectorPort;
    private final int episodeLength;
    private ExecutorService es;
    private long count = 0;

    /**
     * Dalton partitioner for offline data collection.
     *
     * @param metricCollectorPort
     * @param combinerParallelism
     * @param numMetricsPerWorker
     * @param episodeLength
     * @param a
     * @param epsilon
     * @param temperature
     * @param trueHotKeysToWorkers
     */
    public SlidingRouteDaltonStatPartitioner(
            int metricCollectorPort,
            int combinerParallelism,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double a,
            double epsilon,
            double temperature,
            double initialPerf,
            Map<Integer, List<Integer>> trueHotKeysToWorkers,
            int numHotKeys) {

        this.metricCollectorPort = metricCollectorPort;
        this.numMetricsPerWorker = numMetricsPerWorker;
        this.metricNumSlides = metricNumSlides;
        this.combinerParallelism = combinerParallelism;
        this.a = a;
        this.epsilon = epsilon;
        this.episodeLength = episodeLength;
        this.temperature = temperature;
        this.initialPerf = initialPerf;
        this.trueHotKeysToWorkers = trueHotKeysToWorkers;
        this.numHotKeys = numHotKeys;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        StatRewardAssigner<Integer> rewardAssigner = new StatRewardAssigner<>(combinerParallelism);
        cbandit =
                new StaticHotKeyCBandit<>(
                        combinerParallelism,
                        metricNumSlides,
                        trueHotKeysToWorkers,
                        numHotKeys,
                        a,
                        epsilon,
                        temperature,
                        rewardAssigner);
        rewardAssigner.setWorkerStatistics(cbandit.getWorkerStatistics());

        this.coldWorkerStatistics = new WorkerStatistics(metricNumSlides);
        this.metricCollector = new MetricCollector(metricCollectorPort, combinerParallelism);
        this.env =
                new AutoExternalEnv(
                        metricNumSlides,
                        episodeLength,
                        combinerParallelism,
                        numMetricsPerWorker,
                        initialPerf,
                        metricCollector,
                        cbandit.getWorkerStatistics(),
                        this::onNewSlide);
        new Thread(this.env).start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.env.close();
        this.metricCollector.close();
    }

    private synchronized void onNewSlide(NextActionResponse response) {
        long slideId = Objects.nonNull(response) ? response.getRequestStartTs() : 0;
        int[] coldWorkerCounts = coldWorkerStatistics.getWorkerCounts(combinerParallelism);
        LOG.info("Slide {}: cold tuples: {}", slideId, coldWorkerCounts);
        cbandit.createNewSlideManually(slideId);
        coldWorkerStatistics.newSlide();
        LOG.info("Slide {}: count: {}", slideId, count);
    }

    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context ctx,
            Collector<Tuple2<Integer, R>> out) {

        int partition = partition(r, ctx.timestamp());
        out.collect(new Tuple2<>(partition, r));
        cbandit.updateEffect(r, partition);
        if (!r.isHot()) {
            coldWorkerStatistics.add(partition);
        }
        count++;
    }

    private int partition(R r, long ts) {
        return cbandit.partition(r, ts);
    }

    private void printMetrics(Map<Integer, double[]> metrics, long rv) {
        if (metrics != null) {
            double[] nonIdleTimes = getNonIdleTimes(metrics);
            double imbalance = getImbalance(nonIdleTimes);
            LOG.info("Route {} imbalance: {} non-idle times: {}", rv, imbalance, nonIdleTimes);
        } else {
            LOG.error("Route {} metrics timeout", rv);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
