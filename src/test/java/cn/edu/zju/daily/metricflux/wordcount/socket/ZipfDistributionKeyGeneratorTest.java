package cn.edu.zju.daily.metricflux.wordcount.socket;

import static cn.edu.zju.daily.metricflux.genetic.FakeEnvironmentTest.stat;
import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import cn.edu.zju.daily.metricflux.utils.HashUtils;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class ZipfDistributionKeyGeneratorTest {

    static class IntPair implements Comparable<IntPair> {
        int first;
        int second;

        IntPair(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(IntPair o) {
            return Integer.compare(first, o.first);
        }

        @Override
        public String toString() {
            return first + ":" + second;
        }
    }

    private static final int TOTAL = 30000 * 600;
    private static final int NUM_KEYS = 100000;
    private static final int NUM_PARTITIONS = 64;
    private static final double HOT_KEY_FACTOR = 2;

    @Test
    void test() {
        for (int i = 15; i <= 15; i++) {
            double zipf = i / 10.0;
            //            testPartition(zipf);
            getHotCount(zipf);
            getColdDistribution(zipf);
            getColdDistribution(zipf);
        }
    }

    @Test
    void testCoverage() {
        ZipfDistributionKeyGenerator gen = new ZipfDistributionKeyGenerator(100000, 1);
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < TOTAL; i++) {
            int key = gen.next();
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }
        for (int i = 0; i < 10; i++) {
            System.out.println((double) counts.get(i) / TOTAL);
        }
    }

    void testPartition(double zipf) {
        Function<Integer, List<Integer>> f =
                new Function<>() {
                    @Override
                    public List<Integer> apply(Integer key) {
                        // this is specifically for zipf 1.5, 100000 keys
                        if (key == 0) {
                            return IntStream.rangeClosed(0, 30).boxed().collect(toList());
                        } else if (key == 1) {
                            return IntStream.rangeClosed(31, 41).boxed().collect(toList());
                        } else if (key == 2) {
                            return IntStream.rangeClosed(42, 47).boxed().collect(toList());
                        } else if (key == 3) {
                            return IntStream.rangeClosed(48, 51).boxed().collect(toList());
                        } else if (key == 4) {
                            return IntStream.rangeClosed(52, 54).boxed().collect(toList());
                        } else if (key == 5) {
                            return List.of(55, 56);
                        } else if (key == 6) {
                            return List.of(57, 58);
                        } else if (key == 7) {
                            return List.of(59, 59); // hot key
                        } else {
                            // cold key; hash
                            //                    return List.of(key % NUM_PARTITIONS);
                            return Collections.emptyList();
                        }
                    }
                };

        ZipfDistributionKeyGenerator generator =
                new ZipfDistributionKeyGenerator(NUM_KEYS, zipf, false);
        int[] count = new int[NUM_PARTITIONS];
        Random random = new Random();

        for (int i = 0; i < TOTAL; i++) {
            int key = generator.next();
            List<Integer> partitions = f.apply(key);
            if (partitions.isEmpty()) {
                continue;
            } else if (partitions.size() == 1) {
                count[partitions.get(0)]++;
            } else {
                count[partitions.get(random.nextInt(partitions.size()))]++;
            }
        }

        System.out.println(
                "zipf="
                        + zipf
                        + ", range="
                        + range(count)
                        + ", std="
                        + std(count)
                        + ", array="
                        + Arrays.toString(count));
    }

    int getNumHotKeys(Map<Integer, Integer> keyCount) {
        return 10;
        //        int average = TOTAL / NUM_PARTITIONS;
        //        return (int) (keyCount.entrySet().stream().filter(e -> e.getValue() >
        // average).count() * HOT_KEY_FACTOR);
    }

    int getHotCount(double zipf) {
        ZipfDistributionKeyGenerator generator =
                new ZipfDistributionKeyGenerator(NUM_KEYS, zipf, false);
        Map<Integer, Integer> keyCount = new HashMap<>();
        for (int i = 0; i < TOTAL; i++) {
            int key = generator.next();
            keyCount.put(key, keyCount.getOrDefault(key, 0) + 1);
        }
        int numHotKeys = getNumHotKeys(keyCount);
        List<IntPair> list =
                keyCount.entrySet().stream()
                        .map(e -> new IntPair(e.getKey(), e.getValue()))
                        .sorted((a, b) -> Integer.compare(b.second, a.second))
                        .limit(numHotKeys)
                        .collect(toList());
        System.out.println(list);
        System.out.println("Sum: " + list.stream().mapToInt(p -> p.second).sum());
        return list.size();
    }

    void getColdDistribution(double zipf) {
        ZipfDistributionKeyGenerator generator =
                new ZipfDistributionKeyGenerator(NUM_KEYS, zipf, false);
        Map<Integer, Integer> keyCount = new HashMap<>();
        for (int i = 0; i < TOTAL; i++) {
            int key = generator.next();
            keyCount.put(key, keyCount.getOrDefault(key, 0) + 1);
        }
        int numHotKeys = getNumHotKeys(keyCount);
        List<IntPair> list =
                keyCount.entrySet().stream()
                        .map(e -> new IntPair(e.getKey(), e.getValue()))
                        .sorted((a, b) -> Integer.compare(b.second, a.second))
                        .skip(numHotKeys)
                        .collect(toList());
        int[] sum = new int[NUM_PARTITIONS];
        for (IntPair pair : list) {
            int key = pair.first;
            int count = pair.second;
            sum[Math.floorMod(hash(key), NUM_PARTITIONS)] += count;
        }
        stat(sum);
    }

    double std(int[] arr) {
        double mean = Arrays.stream(arr).average().orElse(0);
        double sum = Arrays.stream(arr).mapToDouble(i -> Math.pow(i - mean, 2)).sum();
        return Math.sqrt(sum / arr.length);
    }

    int hash(int key) {
        return HashUtils.hash(key);
    }

    int range(int[] arr) {
        return Arrays.stream(arr).max().orElse(0) - Arrays.stream(arr).min().orElse(0);
    }
}
