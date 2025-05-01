package cn.edu.zju.daily.metricflux.utils;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.selectFromProbabilities;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.partitioner.containerv2.QTable;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;

public class DistributionUtils {

    private static final int NUM_SAMPLES = 100000;

    public static <K> double ksTest(Map<K, Integer> x, Map<K, Integer> y) {
        int xSum = x.values().stream().mapToInt(Integer::intValue).sum();
        int ySum = y.values().stream().mapToInt(Integer::intValue).sum();

        double[] xp =
                x.values().stream()
                        .sorted(reverseOrder())
                        .mapToDouble(i -> (double) i / xSum)
                        .toArray();
        double[] yp =
                y.values().stream()
                        .sorted(reverseOrder())
                        .mapToDouble(i -> (double) i / ySum)
                        .toArray();

        double[] xSamples = new double[NUM_SAMPLES];
        double[] ySamples = new double[NUM_SAMPLES];
        for (int i = 0; i < NUM_SAMPLES; i++) {
            xSamples[i] = selectFromProbabilities(xp);
            ySamples[i] = selectFromProbabilities(yp);
        }

        KolmogorovSmirnovTest ksTest = new KolmogorovSmirnovTest();
        return ksTest.kolmogorovSmirnovTest(xSamples, ySamples);
    }

    public static <K> double euclidian(Map<K, Integer> x, Map<K, Integer> y, int numHotKeys) {
        int xSum = x.values().stream().mapToInt(Integer::intValue).sum();
        int ySum = y.values().stream().mapToInt(Integer::intValue).sum();

        double[] xp =
                x.values().stream()
                        .sorted(reverseOrder())
                        .limit(numHotKeys)
                        .mapToDouble(i -> (double) i / xSum)
                        .toArray();
        double[] yp =
                y.values().stream()
                        .sorted(reverseOrder())
                        .limit(numHotKeys)
                        .mapToDouble(i -> (double) i / ySum)
                        .toArray();

        // append 0
        if (xp.length < numHotKeys) {
            xp = Arrays.copyOf(xp, numHotKeys);
        }
        if (yp.length < numHotKeys) {
            yp = Arrays.copyOf(yp, numHotKeys);
        }

        return euclidian(xp, yp);
    }

    public static double euclidian(double[] x, double[] y) {
        if (x.length != y.length) {
            throw new IllegalArgumentException("size not equal.");
        }
        double sum = 0;
        for (int i = 0; i < x.length; i++) {
            sum += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return Math.sqrt(sum);
    }

    public static void normalize(double[] array) {
        double sum = Arrays.stream(array).sum();
        for (int i = 0; i < array.length; i++) {
            array[i] /= sum;
        }
    }

    public static double[] normalize(int[] array) {
        double sum = Arrays.stream(array).sum();
        return Arrays.stream(array).asDoubleStream().map(i -> i / sum).toArray();
    }

    public static <K> double[] toKeyDistributionArray(List<Pair<K, Integer>> sortedKeyCounts) {
        double[] keyDistribution = sortedKeyCounts.stream().mapToDouble(Pair::getValue).toArray();
        normalize(keyDistribution);
        return keyDistribution;
    }

    public static <K> double[] toKeyDistributionArray(
            List<Pair<K, Integer>> sortedKeyCounts, int size) {
        double[] keyDistribution = toKeyDistributionArray(sortedKeyCounts);
        if (keyDistribution.length < size) {
            return Arrays.copyOf(keyDistribution, size);
        }
        return keyDistribution;
    }

    public static <K> double[][] toWorkerDistributionMatrix(
            List<Pair<K, Integer>> sortedKeyCounts, Map<K, int[]> workerCounts) {
        return sortedKeyCounts.stream()
                .map(
                        pair -> {
                            K key = pair.getKey();
                            int[] counts = workerCounts.get(key);
                            int sum = Arrays.stream(counts).sum();
                            return Arrays.stream(counts)
                                    .mapToDouble(i -> (double) i / sum)
                                    .toArray();
                        })
                .toArray(double[][]::new);
    }

    public static <K> double[][] toNormalizedQTable(
            List<Pair<K, Integer>> sortedKeyCounts, QTable<K> qTable) {
        return sortedKeyCounts.stream()
                .map(pair -> qTable.getOrDefault(pair.getKey()))
                .toArray(double[][]::new);
    }

    public static <K> Map<K, double[]> toQTableMap(
            List<Pair<K, Integer>> sortedKeyCounts, double[][] normalizedQTable) {
        Map<K, double[]> table = new HashMap<>();
        for (int i = 0; i < sortedKeyCounts.size(); i++) {
            table.put(sortedKeyCounts.get(i).getKey(), normalizedQTable[i]);
        }
        return table;
    }

    public static <K> int[][] toAllowedWorkersMatrix(
            List<Pair<K, Integer>> sortedKeyCounts, Map<K, List<Integer>> allowedWorkers) {
        return sortedKeyCounts.stream()
                .map(
                        pair ->
                                allowedWorkers.get(pair.getKey()).stream()
                                        .mapToInt(Integer::intValue)
                                        .toArray())
                .toArray(int[][]::new);
    }

    public static <K> Map<K, List<Integer>> toAllowedWorkersMap(
            List<Pair<K, Integer>> sortedKeyCounts, int[][] allowedWorkers) {
        Map<K, List<Integer>> map = new HashMap<>();
        for (int i = 0; i < sortedKeyCounts.size(); i++) {
            map.put(
                    sortedKeyCounts.get(i).getKey(),
                    Arrays.stream(allowedWorkers[i]).boxed().collect(toList()));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public static <K> List<Integer>[] toAllowedWorkersArray(
            List<Pair<K, Integer>> sortedKeyCounts, Map<K, List<Integer>> allowedWorkers) {
        return (List<Integer>[])
                sortedKeyCounts.stream()
                        .map(pair -> allowedWorkers.get(pair.getKey()))
                        .toArray(List[]::new);
    }

    public static <K> Map<K, List<Integer>> toAllowedWorkersMap(
            List<Pair<K, Integer>> sortedKeyCounts, List<Integer>[] allowedWorkers) {
        Map<K, List<Integer>> map = new HashMap<>();
        for (int i = 0; i < sortedKeyCounts.size(); i++) {
            map.put(sortedKeyCounts.get(i).getKey(), allowedWorkers[i]);
        }
        return map;
    }

    /**
     * Sort hot key counts by frequency in reversed order.
     *
     * @param hotKeyCounts hot key counts
     * @return sorted list
     * @param <K> key type
     */
    public static <K> List<Pair<K, Integer>> sorted(Map<K, Integer> hotKeyCounts) {
        return hotKeyCounts.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .sorted(Comparator.<Pair<K, Integer>>comparingInt(Pair::getValue).reversed())
                .collect(toList());
    }
}
