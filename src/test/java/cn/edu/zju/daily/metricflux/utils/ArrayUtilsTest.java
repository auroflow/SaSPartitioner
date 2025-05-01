package cn.edu.zju.daily.metricflux.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ArrayUtilsTest {

    @Test
    void test() {
        int[] array =
                new int[] {
                    1764, 83, 1415, 1810, 89, 1875, 799, 1979, 1934, 1971, 1740, 1985, 1930, 1860,
                    1949, 1891, 1772, 1742, 1732, 736, 1873, 382, 1924, 1318, 1876, 1063, 1877,
                    1687, 1062, 1946, 1488, 1903, 532, 1594, 1532, 1710, 1723, 1804, 1287, 1641,
                    1460, 1799, 1780, 1957, 1624, 1583, 1635, 1660, 1285, 2495, 753, 1976, 1988,
                    1501, 1653, 1604, 1051, 1733, 667, 2075, 795, 2004, 2597, 844
                };
        double[] normalized = ArrayUtils.normalize(array);
        System.out.println(ArrayUtils.doubleArrayToString(normalized, 3));
    }

    @Test
    void testDoubleArray() {
        double[] array = new double[] {Double.NaN, Double.POSITIVE_INFINITY, Math.PI};
        String str = ArrayUtils.doubleArrayToString(array, 3);
        assertEquals("[NaN, Infinity, 3.142]", str);
    }
}
