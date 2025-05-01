package cn.edu.zju.daily.metricflux.partitioner.baseline;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.zju.daily.metricflux.utils.HashUtils;
import org.junit.jupiter.api.Test;

class HashPartitionerTest {
    @Test
    void test() {
        int key = 1000;
        int numWorkers = 128;
        int partition = Math.floorMod(HashUtils.hash(Integer.hashCode(key)), numWorkers);
        System.out.println(partition);
        // 75
    }
}
