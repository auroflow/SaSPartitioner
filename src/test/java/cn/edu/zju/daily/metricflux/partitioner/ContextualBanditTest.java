package cn.edu.zju.daily.metricflux.partitioner;

import cn.edu.zju.daily.metricflux.utils.ArrayUtils;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class ContextualBanditTest {

    @Test
    void test() {
        double[] array =
                new double[] {
                    810, 514, 530, 29, 530, 128, 880, 530, 524, 138, 29, 29, 465, 29, 465, 960, 450,
                    530, 346, 840, 39, 465, 530, 0, 530, 524, 544, 450, 287, 60, 39, 530, 920, 396,
                    530, 560, 534, 445, 510, 470, 530, 445, 450, 470, 39, 470, 600, 0, 524, 445, 0,
                    0, 450, 59, 39, 445, 450, 475, 39, 0
                };
        double[] softmax = ArrayUtils.softmax(array, 100);
        System.out.println(Arrays.toString(softmax));
    }

    @Test
    void test2() {
        double[] array =
                new double[] {
                    1000, 1000, 871, 0, 1000, 1000, 1000, 1000, 366, 1000, 366, 0, 1000, 1000, 1000,
                    1000, 1000, 920, 0, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000,
                    841, 0, 1000, 366, 1000, 1000, 0, 1000, 1000, 783, 1000, 1000, 960, 0, 1000,
                    1000, 0, 1000, 0, 1000, 0, 1000, 1000, 960, 1000, 376, 1000, 722, 1000, 930, 891
                };
        double[] softmax = ArrayUtils.softmax(array, 100);
        System.out.println(Arrays.toString(softmax));
    }
}
