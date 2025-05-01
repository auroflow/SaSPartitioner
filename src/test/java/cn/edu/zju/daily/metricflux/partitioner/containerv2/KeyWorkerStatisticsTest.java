package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import cn.edu.zju.daily.metricflux.utils.ArrayUtils;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyWorkerStatisticsTest {

    @Test
    void test() {
        KeyWorkerStatistics<Integer> stats = new KeyWorkerStatistics<>(2, 5);

        stats.add(1, 2);
        stats.add(2, 1);
        stats.add(2, 2);

        Map<Integer, double[]> ratios = stats.getWorkerRatios(false);
        System.out.println(ArrayUtils.intDoubleArrayMapToString(ratios));
        System.out.println();

        stats.newSlide();

        stats.add(3, 2);
        ratios = stats.getWorkerRatios(false);
        System.out.println(ArrayUtils.intDoubleArrayMapToString(ratios));
        System.out.println();

        stats.newSlide();

        ratios = stats.getWorkerRatios(false);
        System.out.println(ArrayUtils.intDoubleArrayMapToString(ratios));
        System.out.println();
    }
}
