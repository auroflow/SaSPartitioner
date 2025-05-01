package cn.edu.zju.daily.metricflux.genetic;

import static cn.edu.zju.daily.metricflux.utils.ArrayUtils.cv;

import cn.edu.zju.daily.metricflux.utils.HashUtils;
import java.util.Arrays;

public class FakeEnvironmentTest {

    private static final int NUM_KEYS = 100000;
    private static final double EXP = 1.5;
    private static final int NUM_PARTITIONS = 60;
    private static final int NUM_ELEMENTS = 10000000;

    public static void stat(int[] arr) {
        System.out.println("arr: " + Arrays.toString(arr));
        double max = max(arr);
        double mean = Arrays.stream(arr).average().orElse(0);
        double imbalance = (max - mean) / (1e-4 + mean);
        System.out.println(
                "std: "
                        + std(arr)
                        + ", cv: "
                        + cv(arr)
                        + ", imbalance: "
                        + imbalance
                        + ", max: "
                        + max(arr)
                        + ", min: "
                        + min(arr));
    }

    static double std(int[] arr) {
        double mean = Arrays.stream(arr).average().orElse(0);
        double sum = Arrays.stream(arr).mapToDouble(i -> Math.pow(i - mean, 2)).sum();
        return Math.sqrt(sum / arr.length);
    }

    int hash(int key) {
        return HashUtils.hash(key);
    }

    static int range(int[] arr) {
        return max(arr) - min(arr);
    }

    static int max(int[] arr) {
        return Arrays.stream(arr).max().orElse(0);
    }

    static int min(int[] arr) {
        return Arrays.stream(arr).min().orElse(0);
    }
}
