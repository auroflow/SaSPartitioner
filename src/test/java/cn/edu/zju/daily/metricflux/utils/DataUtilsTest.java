package cn.edu.zju.daily.metricflux.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class DataUtilsTest {

    @Test
    void testReadDistribution() {
        String path = "/home/user/code/data/t4sa-freq-original.csv";
        List<Integer> dist = DataUtils.readDistribution(path);
        System.out.println("Sum: " + dist.stream().mapToInt(Integer::intValue).sum());
        System.out.println("Length: " + dist.size());
    }
}
