package cn.edu.zju.daily.metricflux.utils;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.doubleArrayToString;
import static smile.math.MathEx.sum;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.util.Preconditions;

@Slf4j
public class RoutingTableUtils {

    private static final int ZIPF_N = 100000;
    private static boolean warned = false;

    /**
     * Get the candidate workers for each hot key.
     *
     * @param plan A 2D integer list of size (numWorkers, numHotKeys) where plan[w][k] is the share
     *     of hot key k on worker w.
     * @param hotKeys A list of hot keys.
     */
    public static <K> Map<K, List<Integer>> toCandidateWorkersMap(
            List<List<Integer>> plan, List<K> hotKeys) {

        if (plan == null) {
            return null;
        }

        Map<K, List<Integer>> candidates = new HashMap<>();

        int numHotKeys = plan.get(0).size();
        if (numHotKeys != hotKeys.size()) {
            throw new IllegalArgumentException("The number of hot keys does not match the plan.");
        }
        int numWorkers = plan.size();

        for (int i = 0; i < numHotKeys; i++) {
            K hotKey = hotKeys.get(i);
            List<Integer> workers = new ArrayList<>();
            int distinctWorkers = 0;
            for (int j = 0; j < numWorkers; j++) {
                int shares = plan.get(j).get(i);
                if (shares > 0) {
                    distinctWorkers++;
                    for (int t = 0; t < shares; t++) {
                        workers.add(j);
                    }
                }
            }
            if (distinctWorkers > 1) {
                candidates.put(hotKey, workers);
            } else if (distinctWorkers == 1) {
                candidates.put(hotKey, List.of(workers.get(0)));
            } else {
                throw new IllegalArgumentException("No worker is assigned to hot key " + hotKey);
            }
        }

        return candidates;
    }

    public static <K> Map<K, List<Integer>> toCandidateWorkersMap(int[][] plan, List<K> hotKeys) {
        Map<K, List<Integer>> candidates = new HashMap<>();

        int numHotKeys = plan[0].length;
        if (numHotKeys != hotKeys.size()) {
            throw new IllegalArgumentException("The number of hot keys does not match the plan.");
        }
        int numWorkers = plan.length;

        for (int i = 0; i < numHotKeys; i++) {
            K hotKey = hotKeys.get(i);
            List<Integer> workers = new ArrayList<>();
            int distinctWorkers = 0;
            for (int j = 0; j < numWorkers; j++) {
                int shares = plan[j][i];
                if (shares > 0) {
                    distinctWorkers++;
                    for (int t = 0; t < shares; t++) {
                        workers.add(j);
                    }
                }
            }
            if (distinctWorkers > 1) {
                candidates.put(hotKey, workers);
            } else if (distinctWorkers == 1) {
                candidates.put(hotKey, List.of(workers.get(0)));
            } else {
                throw new IllegalArgumentException("No worker is assigned to hot key " + hotKey);
            }
        }

        return candidates;
    }

    public static long toRouteVersion(String episodeId) {
        return Long.parseLong(episodeId);
    }

    public static String toEpisodeId(long routeVersion) {
        return Long.toString(routeVersion);
    }

    /**
     * Get masks.
     *
     * @param maskIndexes
     * @return An array of allowed (key, worker) pairs.
     */
    public static List<Pair<Integer, Integer>> getMaskFromMaskIndexes(
            List<List<Integer>> maskIndexes) {
        List<Pair<Integer, Integer>> mask = new ArrayList<>();
        List<Integer> starts = maskIndexes.get(0);
        List<Integer> ends = maskIndexes.get(1);
        for (int key = 0; key < starts.size(); key++) {
            for (int partition = starts.get(key); partition < ends.get(key); partition++) {
                mask.add(Pair.of(key, partition));
            }
        }
        return mask;
    }

    public static double[] getZipfProbabilities(double alpha, int size) {
        System.out.println("Getting zipf probability " + alpha);
        double[] probabilities = new double[size];
        if (alpha > 0) {
            ZipfDistribution zipf = new ZipfDistribution(ZIPF_N, alpha);
            ForkJoinPool customThreadPool = new ForkJoinPool();
            try {
                customThreadPool
                        .submit(
                                () ->
                                        IntStream.range(1, size + 1)
                                                .parallel()
                                                .forEach(
                                                        i -> {
                                                            if (i % 1000 == 0) {
                                                                System.out.println(
                                                                        i + " of " + size);
                                                            }
                                                            probabilities[i - 1] =
                                                                    zipf.probability(i);
                                                        }))
                        .get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } finally {
                customThreadPool.shutdown();
            }
            double sum = sum(probabilities);
            for (int i = 0; i < size; i++) {
                probabilities[i] /= sum;
            }
        } else if (alpha == 0) {
            Arrays.fill(probabilities, 1.0 / size);
        } else {
            throw new IllegalArgumentException("Invalid alpha: " + alpha);
        }
        return probabilities;
    }

    public static List<List<Integer>> getMaskIndexesHeuristic(
            int numHotKeys, int numTrueHotKeys, int combinerParallelism, double[] distribution) {
        return getMaskIndexesHeuristic(
                numHotKeys, numTrueHotKeys, 0, combinerParallelism, distribution);
    }

    /**
     * Returns a list that contains two sublists: start (inclusive) and end (exclusive) of the
     * permitted partition IDs for each "true" hot key.
     *
     * @param numHotKeys number of hot keys
     * @param numTrueHotKeys number of "true" hot keys
     * @param spreadKeys number of "true" hot keys without a partition mask
     * @param combinerParallelism combiner parallelism
     * @param d the distribution of all keys
     * @return start and end of the permitted partition IDs for each "true" hot key.
     */
    public static List<List<Integer>> getMaskIndexesHeuristic(
            int numHotKeys,
            int numTrueHotKeys,
            int spreadKeys,
            int combinerParallelism,
            double[] d) {

        Preconditions.checkArgument(numHotKeys > 0, "numHotKeys must be > 0");
        Preconditions.checkArgument(numHotKeys <= d.length, "numHotKeys must be <= d.length");
        Preconditions.checkArgument(
                numTrueHotKeys <= numHotKeys, "numTrueHotKeys must be <= numHotKeys");

        if (numTrueHotKeys <= 0) {
            double trueHotKeyFreq = d[0];
            int index = 1;
            while (index < numHotKeys) {
                // Cannot let the smallest "true" hot key have less than 1 partition (that would be
                // meaningless)
                trueHotKeyFreq += d[index];
                double lastTrueHotKeyPartitions = combinerParallelism * d[index];
                System.out.println(
                        "Were there "
                                + (index + 1)
                                + " true hot keys the last one would get "
                                + (combinerParallelism * d[index] / trueHotKeyFreq)
                                + " workers");
                if (lastTrueHotKeyPartitions <= trueHotKeyFreq) {
                    break;
                }
                index++;
            }

            numTrueHotKeys = index;
        }

        System.out.println(
                "get_mask_indexes_heuristic: num_hot_keys = "
                        + numHotKeys
                        + ", num_true_hot_keys = "
                        + numTrueHotKeys
                        + ", hot key frequency = "
                        + sum(Arrays.copyOfRange(d, 0, numHotKeys)));

        if (spreadKeys > numTrueHotKeys) {
            System.out.println(
                    "Warning: spreadKeys "
                            + spreadKeys
                            + " > numTrueHotKeys "
                            + numTrueHotKeys
                            + ". Setting spreadKeys to "
                            + numTrueHotKeys);
            spreadKeys = numTrueHotKeys;
        }
        List<Integer> starts = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();

        for (int i = 0; i < spreadKeys; i++) {
            starts.add(0);
            ends.add(combinerParallelism);
        }

        if (spreadKeys < numTrueHotKeys) {
            double[] subList = Arrays.copyOfRange(d, spreadKeys, numTrueHotKeys);
            double sum = sum(subList);
            List<Double> cs = new ArrayList<>(Collections.nCopies(1 + spreadKeys, 0.0));

            double cumulativeSum = 0.0;
            for (int i = 0; i < subList.length; i++) {
                cumulativeSum += (subList[i] / sum) * combinerParallelism;
                cs.add(cumulativeSum);
            }

            for (int i = spreadKeys; i < numTrueHotKeys; i++) {
                int startIndex = (int) Math.floor(cs.get(i));
                int endIndex = Math.min(combinerParallelism, (int) Math.ceil(cs.get(i + 1)));
                starts.add(startIndex);
                ends.add(endIndex);
            }
        }

        System.out.println(
                "First 20 mask index starts: " + starts.subList(0, Math.min(20, starts.size())));
        System.out.println(
                "First 20 mask index ends: " + ends.subList(0, Math.min(20, ends.size())));

        return List.of(starts, ends);
    }

    /**
     * Returns a list that contains two sublists: start and end of the permitted partition IDs for
     * each "true" hot key.
     *
     * @param params parameters
     * @return start and end of the permitted partition IDs for each "true" hot key.
     */
    public static List<List<Integer>> getMaskIndexesHeuristic(Parameters params) {
        double[] d;
        int numHotKeys = params.getNumHotKeys();
        if ("zipf".equals(params.getDataset())) {
            double alpha = params.getZipf();
            if (numHotKeys <= 0) {
                // If numHotKeys <= 0, consider all keys as hot keys
                numHotKeys = params.getNumKeys();
            }
            d = getZipfProbabilities(alpha, numHotKeys);

            System.out.println(
                    "Zipf-"
                            + alpha
                            + ": "
                            + doubleArrayToString(
                                    Arrays.copyOfRange(d, 0, Math.min(10, d.length)), 3));
        } else if ("t4sa".equals(params.getDataset()) || "file".equals(params.getDataset())) {
            List<Integer> dist = DataUtils.readDistribution(params.getDatasetDistributionPath());
            if (numHotKeys <= 0) {
                // If numHotKeys <= 0, consider all keys as hot keys
                numHotKeys = dist.size();
            }

            long sum = dist.stream().mapToLong(Integer::longValue).sum();

            d = new double[dist.size()];
            for (int i = 0; i < dist.size(); i++) {
                d[i] = (double) dist.get(i) / sum;
            }

            System.out.println(
                    "T4sa: "
                            + doubleArrayToString(
                                    Arrays.copyOfRange(d, 0, Math.min(10, d.length)), 3));
        } else {
            throw new IllegalArgumentException("Unknown dataset: " + params.getDataset());
        }

        return getMaskIndexesHeuristic(
                numHotKeys,
                params.getNumTrueHotKeys(),
                params.getMaskSpreadKeys(),
                params.getCombinerParallelism(),
                d);
    }

    /**
     * Returns a list of permitted (key, partition) pairs, only containing true hot keys.
     *
     * @param params
     * @return
     */
    public static List<Pair<Integer, Integer>> getMaskHeuristic(Parameters params) {
        List<List<Integer>> maskIndexes = getMaskIndexesHeuristic(params);
        return getMaskFromMaskIndexes(maskIndexes);
    }

    public static List<Integer> getFanouts(
            List<Pair<Integer, Integer>> mask, int numHotKeys, int parallelism) {

        // The hottest keys use masks
        Map<Integer, Integer> fanouts = new HashMap<>();
        for (Pair<Integer, Integer> pair : mask) {
            fanouts.put(pair.getKey(), fanouts.getOrDefault(pair.getKey(), 0) + 1);
        }
        List<Integer> array = new ArrayList<>();
        for (int i = 0; i < fanouts.size(); i++) {
            array.add(fanouts.get(i));
        }

        // Other keys are distributed evenly
        for (int i = fanouts.size(); i < numHotKeys; i++) {
            array.add(parallelism);
        }
        return array;
    }

    public static <K> Map<K, double[]> getRoutingTable(
            List<Pair<Integer, Integer>> mask,
            int numWorkers,
            double[] action,
            List<K> hotKeys,
            int numHotKeys) {
        // hotKeys.size() <= numHotKeys
        Map<Integer, double[]> map = getRoutingTable(mask, numWorkers, action, numHotKeys);
        Map<K, double[]> table = new HashMap<>();
        for (int i = 0; i < hotKeys.size(); i++) {
            if (map.containsKey(i)) {
                table.put(hotKeys.get(i), map.get(i));
            }
        }
        return table;
    }

    public static Map<Integer, double[]> getRoutingTable(
            List<Pair<Integer, Integer>> mask, int numWorkers, double[] action, int numHotKeys) {
        Map<Integer, double[]> routingTable = new HashMap<>();

        if (action.length == 0) {
            return routingTable;
        }

        if (action.length > mask.size()) {
            throw new IllegalArgumentException(
                    "Action length " + action.length + " > mask size " + mask.size() + ".");
        }
        if (action.length < mask.size()) {
            if (!warned) {
                LOG.warn(
                        "Action length {} < mask size {}. Probably some actions are dropped by the RL server.",
                        action.length,
                        mask.size());
                warned = true;
            }
        }
        int actionIdx = 0;
        for (int i = 0; i < mask.size(); i++) {
            Pair<Integer, Integer> pair = mask.get(i);
            int key = pair.getLeft();
            int worker = pair.getRight();
            if (!routingTable.containsKey(key)) {
                routingTable.put(key, new double[numWorkers]);
            }
            // Check if this key has only one worker. Use the fact that `mask` is sorted
            if (action.length < mask.size()
                    && (i == 0 || mask.get(i - 1).getLeft() != key)
                    && (i == mask.size() - 1 || mask.get(i + 1).getLeft() != key)) {
                routingTable.get(key)[worker] = 1.0;
            } else {
                routingTable.get(key)[worker] = action[actionIdx];
                actionIdx++;
            }
        }
        if (actionIdx != action.length) {
            throw new IllegalArgumentException(
                    "Action length "
                            + action.length
                            + " != action index "
                            + actionIdx
                            + " after processing mask.");
        }
        // Normalize
        for (int key : routingTable.keySet()) {
            double[] probs = routingTable.get(key);
            double sum = sum(probs);
            if (sum > 0) {
                for (int i = 0; i < probs.length; i++) {
                    probs[i] /= sum;
                }
            }
        }
        // Evenly distribute the rest of the hot keys
        for (int key = routingTable.size(); key < numHotKeys; key++) {
            double[] probs = new double[numWorkers];
            Arrays.fill(probs, 1.0 / numWorkers);
            routingTable.put(key, probs);
        }
        return routingTable;
    }
}
