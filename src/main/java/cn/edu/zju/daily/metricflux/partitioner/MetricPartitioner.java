package cn.edu.zju.daily.metricflux.partitioner;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.intIntArrayMapToString;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.KeyWorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.containerv2.WorkerStatistics;
import cn.edu.zju.daily.metricflux.partitioner.learning.AutoExternalEnv;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import cn.edu.zju.daily.metricflux.partitioner.learning.MetricCollector;
import cn.edu.zju.daily.metricflux.partitioner.learning.SlidingRouteLearningPartitioner;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Histogram;

/**
 * A partitioner that collects metrics. Note that RayRL based partitioners (e.g. {@link
 * SlidingRouteLearningPartitioner}) do not inherit this class -- they handle metrics by themselves.
 *
 * @param <R>
 */
@Slf4j
public abstract class MetricPartitioner<R extends Record<Integer>> extends Partitioner<Integer, R> {

    private MetricCollector metricCollector;
    private AutoExternalEnv env;
    private WorkerStatistics workerStatistics;
    private WorkerStatistics coldWorkerStatistics;
    private KeyWorkerStatistics<Integer> hotKeyWorkerStatistics;
    private final int metricCollectorPort;
    private final int episodeLength;
    private final int numMetricsPerWorker;
    private final int metricNumSlides;
    private final int combinerParallelism;
    private final double initialPerf;
    private final boolean log;
    private long count = 0;
    private Histogram partitionTime;

    public MetricPartitioner(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            double initialPerf,
            boolean log) {
        this.metricCollectorPort = metricCollectorPort;
        this.numMetricsPerWorker = numMetricsPerWorker;
        this.metricNumSlides = metricNumSlides;
        this.combinerParallelism = numWorkers;
        this.episodeLength = episodeLength;
        this.initialPerf = initialPerf;
        this.log = log;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        this.metricCollector = new MetricCollector(metricCollectorPort, combinerParallelism);
        this.workerStatistics = new WorkerStatistics(metricNumSlides);
        this.coldWorkerStatistics = new WorkerStatistics(metricNumSlides);
        this.hotKeyWorkerStatistics =
                new KeyWorkerStatistics<>(metricNumSlides, combinerParallelism);
        this.env =
                new AutoExternalEnv(
                        metricNumSlides,
                        episodeLength,
                        combinerParallelism,
                        numMetricsPerWorker,
                        initialPerf,
                        metricCollector,
                        workerStatistics,
                        this::onNewSlide);
        new Thread(this.env).start();
    }

    /** Note: OVERRIDING METHODS MUST CALL super.onNewSlide(response)! */
    protected synchronized void onNewSlide(NextActionResponse response) {
        long slideId = Objects.nonNull(response) ? response.getRequestStartTs() : 0;
        int[] workerCounts = workerStatistics.getWorkerCounts(combinerParallelism);
        int[] coldWorkerCounts = coldWorkerStatistics.getWorkerCounts(combinerParallelism);
        LOG.info("Slide {}: count: {}", slideId, count);

        // logging
        if (log) {
            LOG.info("Slide {}: worker statistics: {}", slideId, workerCounts);
            LOG.info("Slide {}: cold tuples: {}", slideId, coldWorkerCounts);
            // This is the real partition table of the previous metric window
            LOG.info(
                    "Slide {}: partition table:\n{}",
                    slideId,
                    intIntArrayMapToString(hotKeyWorkerStatistics.getWorkerCounts(false)));
        }

        workerStatistics.newSlide();
        coldWorkerStatistics.newSlide();
        hotKeyWorkerStatistics.newSlide();
    }

    /**
     * THIS SHOULD BE CALLED BY THE IMPLEMENTATION AFTER ASSIGNING A TUPLE TO A PARTITION!
     *
     * @param key the key of the tuple
     * @param worker the worker to which the tuple was assigned
     * @param isHot whether the key is hot
     */
    protected void applyEffect(int key, int worker, boolean isHot) {
        count++;
        workerStatistics.add(worker);
        if (isHot) {
            hotKeyWorkerStatistics.add(key, worker);
        } else {
            coldWorkerStatistics.add(worker);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.metricCollector.close();
        this.env.close();
    }
}
