package cn.edu.zju.daily.metricflux.utils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/** Parameters regarding Flink job. */
@Data
@NoArgsConstructor
public class Parameters implements Serializable {

    public static Parameters load(String path, boolean isResource) {
        try {
            Yaml yaml = new Yaml(new Constructor(Parameters.class, new LoaderOptions()));
            String url = null;
            if (isResource) {
                if (path.startsWith("/")) {
                    url = path;
                } else {
                    url = "/" + path;
                }
                System.out.println("Reading params from resource " + url);
                return yaml.load(Parameters.class.getResourceAsStream(url));
            } else {
                System.out.println("Reading params from file " + path);
                return yaml.load(Files.newInputStream(Paths.get(path)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ======================
    // Flink
    // ======================
    private String jobManagerHost;
    private int jobManagerPort;

    // ======================
    // Data socket input
    // ======================
    private List<String> inputSocketHosts;
    private List<Integer> inputSocketPorts;

    // ======================
    // Staged rates
    // ======================
    /** Positive means events per second, 0 means no limit, negative means seconds per event. */
    private List<Long> rates;

    private List<Long> rateStages;

    // ======================
    // Pipeline
    // ======================
    private int partitionerParallelism;
    private int combinerParallelism;
    private int reducerParallelism;
    private int combinerWorkloadRatio;

    /** Window slide in milliseconds. */
    private int windowSlide;

    /** Window size in milliseconds. */
    private int windowSize;

    private String hdfsOutputDirectory;

    // ======================
    // Data
    // ======================
    private int numKeys;
    private long numRecords;
    private String dataset;
    private double zipf;
    private List<Integer> numKeysSequence;
    private List<Double> zipfSequence;
    private int maxWordsPerLine;

    /** Distribution shift interval in seconds. */
    private long shiftInterval;

    private long shiftAlignment;
    private boolean shuffleKey;

    /** External datasets */
    private String datasetPath;

    private String datasetDistributionPath;

    // ======================
    // Partitioners
    // ======================
    private boolean disablePartitionerChaining;
    private int partitionerWindowSize;
    private int partitionerWindowSlide;

    /** Interval for downstream operators to update the manual view of their metrics */
    private long routeUpdateIntervalMillis;

    /** preference towards load balance in the reward function (for stat Dalton and DAGreedy) * */
    private double alpha;

    /** Dalton: weight of reward in the update of the qtable */
    private double a;

    /** Dalton: possibility of exploration */
    private double epsilon;

    /** Used to control the number of hot keys */
    @Deprecated private double hotThresholdFactor;

    /**
     * Number of hot keys.
     *
     * <p>There are three kinds of keys for key-splitting algorithms:
     *
     * <ol>
     *   <li>"True" hot keys: that will be routed by advanced algorithms, and will go through
     *       reducers.
     *   <li>"Non-True" hot keys: that will be hashed to the respective reducers.
     *   <li>Non-hot keys: that will be directly hashed to the downstream operator via side output.
     * </ol>
     *
     * <p>This parameter controls the number of "True" and "Non-True" hot keys. If less than or
     * equals to 0, consider all keys as hot keys.
     */
    private int numHotKeys;

    /** Contextual bandit: history size for counting key frequency */
    private int historySize;

    /** Contextual bandit: metric collect interval in millis */
    private int metricCollectInterval;

    /**
     * 1 means the hottest key gets all workers, other keys get workers proportional to their
     * hotness a very large value will make every key get all workers
     */
    private double splitFactorFraction;

    /**
     * Used to quantize the metric values, 0 means no quantum, Metrics usually range from 0 to 1000.
     */
    private int metricQuantum;

    /**
     * DaltonStatPartitionerV2: temperature for selecting q-learning actions, Q values range from 0
     * to 1.
     */
    private double temperature;

    /** Backlog aware partitioner: data rate change detection interval */
    private long dataRateLevelUpdateInterval;

    /** For the manual routing table */
    private List<List<Integer>> routingTable;

    // ------------------
    // Learning-related
    // ------------------
    private String rayServerHost;
    private int rayServerPort;
    private int initialRouteVersion;
    private int metricCollectorPort;
    private int episodeLength;
    private int numMetricsPerWorker;
    private long metricWindowSizeMillis;
    private long metricWindowSlideMillis;
    private double initialPerf;

    /**
     * Number of "True" hot keys. If less than or equals to 0, choose from hot keys whose frequency
     * among hot keys is greater than 1 / combinerParallelism.
     */
    private int numTrueHotKeys;

    /**
     * Number of "True" hot keys without a partition mask. The remaining "True" hot keys will be
     * masked according to their frequencies.
     */
    private int maskSpreadKeys;

    // ======================
    // Throughput experiment
    // ======================
    private String partitioner;
    private List<Long> warmupRates;
    private List<Long> warmupIntervalsSeconds;
    private long initialRate;
    private long rateIntervalSeconds;
    private long rateStep;
    private long maxRate;

    // ======================
    // Route learning
    // ======================
    // "dalton-stat", "sliding-route-learning", "route-learning"
    private String learningPartitioner;
    private int dataRateForLearning;
    private boolean doTest;
    private boolean testOnlyOnce;
    private long firstAdaptiveDurationSec;
    private long nextAdaptiveDurationSec;

    // =======================
    // Distribution shift test
    // =======================
    private List<Double> alphaValues;
    private List<String> rayServerHosts;
    private List<Integer> rayServerPorts;
    private int stressTestRate;
    private String stressTestPartitioner;
    private long stressTestDurationSeconds;
    private boolean partitionerOnly;
}
