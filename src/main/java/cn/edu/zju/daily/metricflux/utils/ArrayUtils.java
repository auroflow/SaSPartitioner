package cn.edu.zju.daily.metricflux.utils;

import static smile.math.MathEx.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.DoubleStream;

public class ArrayUtils {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ArrayUtils.class);

    public static String doubleArrayToString(double[] array, int precision) {
        if (Objects.isNull(array)) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < array.length; i++) {
            sb.append(String.format("%." + precision + "f", array[i]));
            if (i < array.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static String intIntArrayMapToString(Map<Integer, int[]> map) {
        if (map.isEmpty()) {
            return "empty";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, int[]> entry : map.entrySet()) {
            sb.append(String.format("%3d", entry.getKey())).append(": ");
            sb.append(Arrays.toString(entry.getValue()));
            sb.append("\n");
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static String intDoubleArrayMapToString(Map<Integer, double[]> map) {
        if (map.isEmpty()) {
            return "empty";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, double[]> entry : map.entrySet()) {
            sb.append(String.format("%3d", entry.getKey())).append(": ");
            sb.append(doubleArrayToString(entry.getValue(), 3));
            sb.append("\n");
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static double cv(int[] array) {
        double mean = Arrays.stream(array).average().orElseThrow();
        double sum = 0;
        for (int i : array) {
            sum += (i - mean) * (i - mean);
        }
        return Math.sqrt(sum / array.length) / mean;
    }

    public static double[] softmax(double[] input, double temperature) {
        if (temperature <= 0) {
            throw new IllegalArgumentException("temperature must be positive");
        }

        // Step 1: Find the maximum value in the input array for numerical stability
        double max = Arrays.stream(input).max().orElse(Double.NEGATIVE_INFINITY);

        // Step 2: Subtract the max value from each element and exponentiate
        double[] expValues = new double[input.length];
        double sum = 0.0;
        for (int i = 0; i < input.length; i++) {
            expValues[i] = Math.exp((input[i] - max) / temperature);
            sum += expValues[i];
        }

        // Step 3: Normalize by dividing each exponentiated value by the sum
        double[] softmax = new double[input.length];
        for (int i = 0; i < input.length; i++) {
            softmax[i] = expValues[i] / sum;
        }

        // randomly log
        if (Math.random() < 0.0001) {
            LOG.debug(
                    "Softmax: {} -> {}",
                    doubleArrayToString(input, 3),
                    doubleArrayToString(softmax, 3));
        }
        return softmax;
    }

    public static int selectFromProbabilities(double[] probabilities) {
        double random = Math.random();
        double cumsum = 0.0;
        for (int i = 0; i < probabilities.length; i++) {
            cumsum += probabilities[i];
            if (random < cumsum) {
                return i;
            }
        }
        return probabilities.length - 1;
    }

    /**
     * Quantize a double value.
     *
     * @param value value
     * @param quantum the quantum (0 means no quantization)
     * @return quantized value
     */
    public static double quantize(double value, double quantum) {
        if (quantum == 0) {
            return value;
        }
        return Math.round(value / quantum) * quantum;
    }

    /**
     * Quantize a double array.
     *
     * @param value value
     * @param quantum the quantum (0 means no quantization)
     * @return quantized value
     */
    public static double[] quantize(double[] value, double quantum) {
        if (quantum == 0) {
            return value;
        }
        return DoubleStream.of(value).map(v -> quantize(v, quantum)).toArray();
    }

    public static double[] normalize(int[] array) {
        double mean = mean(array);
        double std = sd(array);

        double rangeFactor = 5 * std;

        double[] normalized = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            if (rangeFactor == 0) {
                normalized[i] = 1;
            } else {
                normalized[i] = (array[i] - mean) / rangeFactor + 1;
            }
        }
        return normalized;
    }

    public static void crop(double[] array, int minval, int maxval) {
        for (int i = 0; i < array.length; i++) {
            array[i] = Math.min(maxval, Math.max(minval, array[i]));
        }
    }

    public static double[] concat(double[] a, double[] b) {
        double[] result = new double[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
