package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.intIntArrayMapToString;
import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.selectFromProbabilities;
import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.softmax;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.core.data.Record;
import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticHotKeyCBandit<R extends Record<? extends Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticHotKeyCBandit.class);
    private static final double INITIAL_Q_VALUE = 0.0;

    private final int numWorkers;
    private final Duration windowSlide;
    private final int numHotKeys;
    @Getter private final KeyStatistics<Integer> keyStatistics;
    @Getter private final WorkerStatistics workerStatistics;
    private final KeyWorkerStatistics<Integer> keyWorkerStatistics; // only for hot keys
    private final Map<Integer, List<Integer>> trueHotKeysToWorkers;
    @Getter private final QTable<Integer> qTable;
    private final double epsilon;
    private final double temperature;
    private final HashFamily<Integer> hashFamily;
    private final BiFunction<Integer, Integer, Double> rewardAssigner;
    private final Random random;
    private final List<Integer> allWorkers;
    private long nextUpdateTS;

    /**
     * Constructs a bandit that updates windows automatically.
     *
     * @param numWorkers combiner parallelism
     * @param windowSize windows size
     * @param windowSlide windows slide
     * @param trueHotKeysToWorkers hot keys and their allowed workers
     * @param a weight of reward in the update of the qtable
     * @param epsilon possibility of exploration
     * @param temperature randomness of action selection, 0 always selects the best
     * @param rewardAssigner reward assigner
     */
    public StaticHotKeyCBandit(
            int numWorkers,
            Duration windowSize,
            Duration windowSlide,
            Map<Integer, List<Integer>> trueHotKeysToWorkers,
            int numHotKeys,
            double a,
            double epsilon,
            double temperature,
            BiFunction<Integer, Integer, Double> rewardAssigner) {
        this.numWorkers = numWorkers;
        this.windowSlide = windowSlide;
        // window size should be n times the slide size, n is an integer
        int numSlides = (int) Math.ceil((double) windowSize.toMillis() / windowSlide.toMillis());
        this.trueHotKeysToWorkers = Collections.unmodifiableMap(trueHotKeysToWorkers);
        this.numHotKeys = numHotKeys;
        this.keyStatistics = new KeyStatistics<>(numSlides, numWorkers);
        this.workerStatistics = new WorkerStatistics(numSlides);
        this.keyWorkerStatistics = new KeyWorkerStatistics<>(numSlides, numWorkers);
        this.hashFamily = new HashFamily<>(numWorkers);
        this.epsilon = epsilon;
        this.qTable = new QTable<>(numWorkers, a, INITIAL_Q_VALUE);
        this.nextUpdateTS = 0;
        this.rewardAssigner = rewardAssigner;
        this.temperature = temperature;
        this.random = new Random();
        this.allWorkers = IntStream.range(0, numWorkers).boxed().collect(toList());
        initializeQTable();
    }

    /** Constructs a bandit that requires manual creation of new slides. */
    public StaticHotKeyCBandit(
            int numWorkers,
            int numSlides,
            Map<Integer, List<Integer>> trueHotKeysToWorkers,
            int numHotKeys,
            double a,
            double epsilon,
            double temperature,
            BiFunction<Integer, Integer, Double> rewardAssigner) {
        this.numWorkers = numWorkers;
        this.windowSlide = null;
        // window size should be n times the slide size, n is an integer
        this.trueHotKeysToWorkers = Collections.unmodifiableMap(trueHotKeysToWorkers);
        this.numHotKeys = numHotKeys;
        this.keyStatistics = new KeyStatistics<>(numSlides, numWorkers);
        this.workerStatistics = new WorkerStatistics(numSlides);
        this.keyWorkerStatistics = new KeyWorkerStatistics<>(numSlides, numWorkers);
        this.hashFamily = new HashFamily<>(numWorkers);
        this.epsilon = epsilon;
        this.qTable = new QTable<>(numWorkers, a, INITIAL_Q_VALUE);
        this.nextUpdateTS = 0;
        this.rewardAssigner = rewardAssigner;
        this.temperature = temperature;
        this.random = new Random();
        this.allWorkers = IntStream.range(0, numWorkers).boxed().collect(toList());
        initializeQTable();
    }

    public int partition(R record, long ts) {
        int key = record.getKey();
        if (shouldPeriodicallyCreateSlides()) {
            createNewSlideIfExpired(ts);
        }
        boolean isHot = isHot(key);
        record.setHot(isHot);
        return isHot ? partitionHot(record) : partitionCold(key);
    }

    public int partition(R record) {
        long ts = record.getTs();
        return partition(record, ts);
    }

    /**
     * This method has to be called after the tuple is sent to the downstream collector, whether hot
     * or cold.
     */
    public synchronized void updateEffect(R record, int worker) {
        int key = record.getKey();
        keyStatistics.add(key);
        workerStatistics.add(worker);
        if (isHot(key)) {
            keyWorkerStatistics.add(key, worker);
            if (qTable.containsKey(key)) {
                qTable.update(key, worker, rewardAssigner.apply(key, worker));
            }
        }
    }

    private boolean isHot(int key) {
        return key < numHotKeys;
    }

    private int partitionHot(R record) {
        int key = record.getKey();
        List<Integer> allowedWorkers = trueHotKeysToWorkers.get(key);

        if (allowedWorkers == null) {
            // this hot key is not "true"; return random worker
            return randomElement(allWorkers);
        }

        if (allowedWorkers.size() == 1) {
            // one worker only; actually a cold key
            // record.setHot(false);
            return allowedWorkers.get(0);
        }

        double r = random.nextDouble();
        if (r < 1.0 - epsilon) { // exploitation
            if (this.temperature == 0) {
                return selectBest(allowedWorkers, qTable.get(key));
            } else {
                return selectSoft(allowedWorkers, qTable.get(key), this.temperature);
            }
        } else { // exploration
            return randomAllowedWorker(key);
        }
    }

    private int selectBest(List<Integer> allowedWorkers, double[] qvalues) {
        // the bigger, the better
        double max = -Double.MAX_VALUE;
        int selectedWorker = -1;
        for (int worker : allowedWorkers) {
            if (qvalues[worker] > max) {
                max = qvalues[worker];
                selectedWorker = worker;
            }
        }
        return selectedWorker;
    }

    private int selectSoft(List<Integer> allowedWorkers, double[] qvalues, double temperature) {
        double[] allowedQvalues = new double[allowedWorkers.size()];
        for (int i = 0; i < allowedWorkers.size(); i++) {
            allowedQvalues[i] = qvalues[allowedWorkers.get(i)];
        }
        double[] probabilities = softmax(allowedQvalues, temperature);
        int selectedIndex = selectFromProbabilities(probabilities);
        return allowedWorkers.get(selectedIndex);
    }

    private int partitionCold(int key) {
        return Math.floorMod(hash(key), numWorkers);
    }

    private int randomAllowedWorker(int key) {
        List<Integer> allowedWorkers = trueHotKeysToWorkers.get(key);
        return randomElement(allowedWorkers);
    }

    private void createNewSlideIfExpired(long currentTS) {

        if (currentTS >= nextUpdateTS) {
            createNewSlideManually(currentTS);
            // nextUpdateTS should be aligned with the window slide
            nextUpdateTS = currentTS + windowSlide.toMillis() - currentTS % windowSlide.toMillis();
        }
    }

    /**
     * Create a new slide manually.
     *
     * @param slideId slide ID (for logging)
     */
    public synchronized void createNewSlideManually(long slideId) {

        int[] workerCounts = workerStatistics.getWorkerCounts(numWorkers);

        // logging
        LOG.info("Slide {}: worker statistics: {}", slideId, workerCounts);
        LOG.info(
                "Slide {}: partition table:\n{}",
                slideId,
                intIntArrayMapToString(keyWorkerStatistics.getWorkerCounts(false)));

        keyStatistics.newSlide();
        workerStatistics.newSlide();
        keyWorkerStatistics.newSlide();
    }

    private void initializeQTable() {

        // update qTable
        for (int key : trueHotKeysToWorkers.keySet()) {
            qTable.addKeyIfNotPresent(key);
        }
    }

    private int hash(int key) {
        return hashFamily.baseHash(key);
    }

    private <T> T randomElement(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    private boolean shouldPeriodicallyCreateSlides() {
        return Objects.nonNull(windowSlide);
    }
}
