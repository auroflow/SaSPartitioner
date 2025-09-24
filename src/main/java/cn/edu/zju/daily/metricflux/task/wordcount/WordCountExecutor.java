package cn.edu.zju.daily.metricflux.task.wordcount;

import static cn.edu.zju.daily.metricflux.utils.RoutingTableUtils.*;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.core.KeySplittingPipeline;
import cn.edu.zju.daily.metricflux.core.operator.UniformReduceKeyExtractor;
import cn.edu.zju.daily.metricflux.partitioner.Partitioner;
import cn.edu.zju.daily.metricflux.partitioner.baseline.*;
import cn.edu.zju.daily.metricflux.partitioner.learning.SlidingRouteAdaptivePartitioner;
import cn.edu.zju.daily.metricflux.partitioner.learning.SlidingRouteDaltonStatPartitioner;
import cn.edu.zju.daily.metricflux.partitioner.learning.SlidingRouteLearningPartitioner;
import cn.edu.zju.daily.metricflux.task.wordcount.data.WordCountRecord;
import cn.edu.zju.daily.metricflux.task.wordcount.data.WordCountState;
import cn.edu.zju.daily.metricflux.task.wordcount.operator.FixedIntervalWordCountCombiner;
import cn.edu.zju.daily.metricflux.task.wordcount.operator.WordCountCombiner;
import cn.edu.zju.daily.metricflux.task.wordcount.operator.WordCountReducer;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Word count executor. */
public class WordCountExecutor {

    /**
     * Execute the experiment.
     *
     * <p>Supported partitioners:
     *
     * <ol>
     *   <li>{@code hash}: Hash partitioner
     *   <li>{@code cam}: cAM partitioner
     *   <li>{@code dagreedy}: DAGreedy partitioner
     *   <li>{@code dalton-original}: Dalton partitioner
     *   <li>{@code dalton-metrics}: Dalton partitioner using system metrics
     *   <li>{@code dalton-offline}: A modified Dalton partitioner for offline training data
     *       collection
     *   <li>{@code saspartitioner}: SaSPartitioner
     *   <li>{@code saspartitioner-adaptive}: Workload-aware SaSPartitioner
     * </ol>
     *
     * @param params params
     * @param sourceFunction source function
     * @param partitionerName partition name
     * @param experimentName experiment name
     * @throws Exception
     */
    public static void execute(
            Parameters params,
            SourceFunction<WordCountRecord> sourceFunction,
            String partitionerName,
            String experimentName)
            throws Exception {
        int numWorkers = params.getCombinerParallelism();
        int workloadRatio = params.getCombinerWorkloadRatio();
        String jobManagerHost = params.getJobManagerHost();
        int jobManagerPort = params.getJobManagerPort();
        int numHotKeys = params.getNumHotKeys();

        Partitioner<Integer, WordCountRecord> partitioner;
        WordCountCombiner combiner;
        KeySelector<WordCountState, Integer> reduceKeySelector;
        WordCountReducer reducer = new WordCountReducer(params.getWindowSize());
        KeySplittingPipeline<Integer, WordCountRecord, WordCountState, WordCountState> pipeline;

        List<List<Integer>> maskIndexes;
        List<Integer> starts;
        List<Integer> ends;
        List<Pair<Integer, Integer>> mask;

        // distribution-aware partitioners
        boolean isDistributionAware = partitionerName.equals("saspartitioner");
        boolean masked = isDistributionAware || "dalton-offline".equals(partitionerName);

        if (masked) {
            maskIndexes = getMaskIndexesHeuristic(params);
            starts = maskIndexes.get(0);
            ends = maskIndexes.get(1);
            if (params.getNumHotKeys() <= 0) {
                numHotKeys = starts.size();
            }
            mask = getMaskFromMaskIndexes(maskIndexes);
        } else {
            maskIndexes = null;
            starts = null;
            ends = null;
            mask = null;
        }

        // Partitioner
        if ("hash".equals(partitionerName)) {
            // Baseline: Hash
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new HashPartitioner<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getInitialPerf());
        } else if ("drifting-key".equals(partitionerName)) {
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new DriftingKeyPartitioner<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getInitialPerf(),
                            params.getNumKeys());
        } else if ("cam".equals(partitionerName)) {
            // Baseline: CAM
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new CAM<>(
                            params.getWindowSize(),
                            params.getWindowSlide(),
                            numWorkers,
                            params.getMetricCollectorPort(),
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getInitialPerf());
        } else if ("dagreedy".equals(partitionerName)) {
            // Baseline: DAGreedy
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new DAGreedy<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getWindowSlide(),
                            params.getWindowSize(),
                            params.getNumKeys(),
                            params.getInitialPerf());
        } else if ("flexd".equals(partitionerName)) {
            // Baseline: FlexD
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new FlexD<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getWindowSlide(),
                            params.getWindowSize(),
                            params.getNumKeys(),
                            params.getInitialPerf());
        } else if ("dalton-original".equals(partitionerName)) {
            // Baseline: Dalton
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new DaltonOriginal<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getWindowSlide(),
                            params.getWindowSize(),
                            params.getNumKeys(),
                            params.getInitialPerf());
        } else if ("dalton-metrics".equals(partitionerName)) {
            // Baseline: Dalton
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new DaltonMetrics<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getWindowSlide(),
                            params.getWindowSize(),
                            params.getNumKeys(),
                            params.getInitialPerf());
        } else if ("dalton-offline".equals(partitionerName)) {
            // Dalton-stat: used for offline data collection
            Map<Integer, List<Integer>> trueHotKeysToWorkers = new HashMap<>();
            for (int key = 0; key < starts.size(); key++) {
                List<Integer> workers = new ArrayList<>();
                for (int worker = starts.get(key); worker < ends.get(key); worker++) {
                    workers.add(worker);
                }
                trueHotKeysToWorkers.put(key, workers);
            }

            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new SlidingRouteDaltonStatPartitioner<>(
                            params.getMetricCollectorPort(),
                            numWorkers,
                            metricWindowNumSlides,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            params.getA(),
                            params.getEpsilon(),
                            params.getTemperature(),
                            params.getInitialPerf(),
                            trueHotKeysToWorkers,
                            numHotKeys);
        } else if ("saspartitioner".equals(partitionerName)) {
            // Sliding route learning: new version of learning partitioner, supports sliding metric
            // window
            if (Math.floorMod(
                            params.getMetricWindowSizeMillis(), params.getMetricWindowSlideMillis())
                    != 0) {
                throw new IllegalArgumentException(
                        "Metric window size must be a multiple of the window slide.");
            }

            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            partitioner =
                    new SlidingRouteLearningPartitioner<>(
                            params.getRayServerHost(),
                            params.getRayServerPort(),
                            params.getMetricCollectorPort(),
                            numWorkers,
                            numHotKeys,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            metricWindowNumSlides,
                            params.getInitialPerf(),
                            mask);
        } else if ("saspartitioner-adaptive".equals(partitionerName)) {
            int metricWindowNumSlides =
                    (int)
                            Math.floorDiv(
                                    params.getMetricWindowSizeMillis(),
                                    params.getMetricWindowSlideMillis());

            List<List<Pair<Integer, Integer>>> masks = new ArrayList<>();
            List<double[]> dists = new ArrayList<>();

            List<Double> zipfSequence = params.getAlphaValues();
            for (double alpha : zipfSequence) {
                if (alpha > 0) {
                    double[] dist = getZipfProbabilities(alpha, numHotKeys);
                    List<List<Integer>> mi =
                            getMaskIndexesHeuristic(
                                    numHotKeys,
                                    params.getNumTrueHotKeys(),
                                    params.getCombinerParallelism(),
                                    dist);
                    List<Pair<Integer, Integer>> m = getMaskFromMaskIndexes(mi);
                    masks.add(m);
                    dists.add(dist);
                } else {
                    masks.add(null);
                    dists.add(getZipfProbabilities(0, numHotKeys));
                }
            }

            partitioner =
                    new SlidingRouteAdaptivePartitioner<>(
                            params.getRayServerHosts(),
                            params.getRayServerPorts(),
                            masks,
                            dists,
                            params.getMetricCollectorPort(),
                            numWorkers,
                            numHotKeys,
                            params.getNumMetricsPerWorker(),
                            params.getEpisodeLength(),
                            metricWindowNumSlides,
                            params.getInitialPerf());
        } else {
            throw new IllegalArgumentException("Unknown partitioner: " + partitionerName);
        }

        // Combiner
        combiner =
                new FixedIntervalWordCountCombiner(
                        workloadRatio,
                        jobManagerHost,
                        jobManagerPort,
                        params.getMetricCollectorPort(),
                        Duration.ofMillis(params.getMetricWindowSlideMillis()));

        // Reduce key selector
        if (isDistributionAware) {
            reduceKeySelector =
                    new UniformReduceKeyExtractor<>(
                            params.getReducerParallelism(),
                            IntStream.range(0, numHotKeys).boxed().collect(toList()),
                            IntStream.range(0, numHotKeys)
                                    .map(
                                            i -> {
                                                if (i < ends.size()) {
                                                    return ends.get(i) - starts.get(i);
                                                } else {
                                                    return params.getCombinerParallelism();
                                                }
                                            })
                                    .boxed()
                                    .collect(toList()));
        } else {
            reduceKeySelector = WordCountState::getKey;
        }

        // Pipeline
        pipeline =
                new KeySplittingPipeline<>(
                        params,
                        partitioner,
                        combiner,
                        reducer,
                        reduceKeySelector,
                        new TypeHint<Integer>() {},
                        new TypeHint<WordCountState>() {},
                        new TypeHint<WordCountState>() {},
                        false);

        // Run the pipeline
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(numWorkers);

        String slotSharingGroupName =
                params.isDisablePartitionerChaining() ? "source" : "source_partitioner";
        SingleOutputStreamOperator<WordCountRecord> source =
                env.addSource(sourceFunction, "SocketSource")
                        .slotSharingGroup(slotSharingGroupName)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                        .slotSharingGroup(slotSharingGroupName)
                        .uid("source");

        DataStream<?> result = pipeline.transform(source);

        result.addSink(KeySplittingPipeline.getSink(params, true))
                .setParallelism(numWorkers)
                .setMaxParallelism(numWorkers)
                .name("ResultSink")
                .slotSharingGroup("combiner")
                .uid("sink")
                .disableChaining();

        try {
            env.execute(experimentName + ": " + partitioner);
        } catch (JobCancellationException e) {
            Thread.sleep(10000);
        }
    }
}
