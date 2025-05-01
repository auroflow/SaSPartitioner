package cn.edu.zju.daily.metricflux.utils;

import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import java.util.*;
import org.junit.jupiter.api.Test;

public class DistributionUtilsTest {

    private static final int NUM_ELEMENTS = 100000;

    @Test
    void test() {

        Map<Integer, Integer> map1 = new HashMap<>();
        Map<Integer, Integer> map2 = new HashMap<>();

        fill(map1, 1.5);
        fill(map2, 1.5);

        double ks = DistributionUtils.ksTest(map1, map2);
        System.out.println("KS test value: " + ks);

        double euc = DistributionUtils.euclidian(map1, map2, 10);
        System.out.println("Euclidian distance: " + euc);
    }

    void fill(Map<Integer, Integer> map, double zipf) {
        ZipfDistributionKeyGenerator generator =
                new ZipfDistributionKeyGenerator(100000, zipf, false);

        for (int i = 0; i < NUM_ELEMENTS; i++) {
            int el = generator.next();
            if (el < 8) {
                map.put(el, map.getOrDefault(el, 0) + 1);
            }
        }
    }
}
