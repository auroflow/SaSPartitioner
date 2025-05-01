package cn.edu.zju.daily.metricflux.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HashUtilsTest {

    @Test
    void testHashPartition() {

        int[] keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
        int[] partitions = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        for (int k = 0; k < 5; k++) {
            for (int i = 0; i < keys.length; i++) {
                int key = keys[i];
                int partition = HashUtils.hashPartition(key + k * 20, 10);
                assertEquals(partitions[i], partition);
            }
        }
    }
}
