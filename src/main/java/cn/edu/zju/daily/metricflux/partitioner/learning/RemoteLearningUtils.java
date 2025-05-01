package cn.edu.zju.daily.metricflux.partitioner.learning;

import static java.lang.Math.abs;
import static smile.math.MathEx.*;

import java.util.HashMap;
import java.util.Map;

public class RemoteLearningUtils {

    public static final long PENDING_UPDATE_ROUTE_VERSION = -1L;
    public static final int METRIC_REPORT_MAGIC_NUMBER = 0x37434d64;
    private static final double EPS = 1e-6;

    public static Map<String, Object> getInfo(double imbalance, double nmad) {
        Map<String, Object> info = new HashMap<>();
        info.put("imbalance", imbalance);
        info.put("negative_imbalance", -imbalance);
        info.put("nmad", nmad);
        return info;
    }

    public static double getImbalance(double[] nonIdleTimes) {
        double max = max(nonIdleTimes);
        double mean = mean(nonIdleTimes);
        double imbalance = (max - mean) / (mean + EPS);
        if (imbalance < -EPS) {
            throw new IllegalArgumentException("Negative imbalance: " + imbalance);
        }
        return imbalance;
    }

    // Get Normalized Mean Absolute Deviation
    public static double getNmad(double[] nonIdleTimes) {
        double mean = mean(nonIdleTimes);
        double sum = 0;
        for (double nonIdleTime : nonIdleTimes) {
            sum += abs(nonIdleTime - mean);
        }
        return sum / (nonIdleTimes.length * mean);
    }

    public static double[] getNonIdleTimes(Map<Integer, double[]> metrics) {
        // For now, we assume the last metric element of each worker represents non-idle time.
        int numWorkers = metrics.size();
        double[] nonIdleTimes = new double[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            nonIdleTimes[i] = metrics.get(i)[metrics.get(i).length - 1];
        }
        return nonIdleTimes;
    }

    public static double[] flatten(Map<Integer, double[]> metrics) {
        int metricsWidth = metrics.get(0).length;
        int numWorkers = metrics.size();
        double[] flatMetrics = new double[metrics.size() * metricsWidth];
        for (int i = 0; i < numWorkers; i++) {
            System.arraycopy(metrics.get(i), 0, flatMetrics, i * metricsWidth, metricsWidth);
        }
        return flatMetrics;
    }
}
