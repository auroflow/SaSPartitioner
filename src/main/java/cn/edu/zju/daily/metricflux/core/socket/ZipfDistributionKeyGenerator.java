package cn.edu.zju.daily.metricflux.core.socket;

import static java.util.stream.Collectors.toList;

import java.util.*;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfDistributionKeyGenerator implements Iterator<Integer> {

    private static final int NUM_TOP_KEYS = 10;

    private final List<IntegerDistribution> distributions;
    private final List<List<Integer>> mappings;
    private final long shiftInterval; // seconds
    private long alignment = 30; // seconds
    private long start;
    private final boolean aligned; // aligned to ts % shiftInterval == 0
    private boolean initialized = false;
    private final int[] counts;
    private int total;

    private long lastUpdateTS = 0;

    public ZipfDistributionKeyGenerator(int numKey, double exponent) {
        this(List.of(numKey), List.of(exponent), Long.MAX_VALUE, 0, false);
    }

    /**
     * @param numKey
     * @param exponent
     */
    public ZipfDistributionKeyGenerator(int numKey, double exponent, boolean shuffle) {
        this(List.of(numKey), List.of(exponent), Long.MAX_VALUE, 0, shuffle);
    }

    public static IntegerDistribution getDistribution(int numKeys, double exponent) {
        if (exponent < 0) {
            throw new IllegalArgumentException("exponents must be non-negative");
        } else if (exponent == 0) {
            return new UniformIntegerDistribution(1, numKeys);
        } else {
            return new ZipfDistribution(numKeys, exponent);
        }
    }

    /**
     * @param numKeys
     * @param exponents
     * @param shiftInterval in seconds.
     * @param alignment in seconds. Ignored if alignment <= 0
     */
    public ZipfDistributionKeyGenerator(
            List<Integer> numKeys, List<Double> exponents, long shiftInterval, long alignment) {
        this(numKeys, exponents, shiftInterval, alignment, true);
    }

    /**
     * @param numKeys
     * @param exponents
     * @param shiftInterval in seconds.
     * @param alignment in seconds. Ignored if alignment <= 0
     */
    public ZipfDistributionKeyGenerator(
            List<Integer> numKeys,
            List<Double> exponents,
            long shiftInterval,
            long alignment,
            boolean shuffle) {
        if (numKeys.size() != exponents.size()) {
            throw new IllegalArgumentException("numKeys and exponents must have the same size");
        }
        this.distributions =
                IntStream.range(0, numKeys.size())
                        .mapToObj(
                                i -> {
                                    int numKey = numKeys.get(i);
                                    double exponent = exponents.get(i);
                                    return getDistribution(numKey, exponent);
                                })
                        .collect(toList());
        this.mappings = new ArrayList<>();
        Random random = new Random(38324);
        for (int numKey : numKeys) {
            List<Integer> list = IntStream.range(0, numKey + 1).boxed().collect(toList());
            if (shuffle) {
                Collections.shuffle(list, new Random(random.nextLong()));
            }
            mappings.add(list);
        }
        this.shiftInterval = shiftInterval;
        this.start = System.currentTimeMillis();
        this.aligned = alignment > 0;
        this.alignment = alignment;
        this.counts = new int[NUM_TOP_KEYS];
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    private int nextIndex() {
        long elapsed = System.currentTimeMillis() - start;
        return (int) Math.floorMod(elapsed / (1000 * shiftInterval), distributions.size());
    }

    private void initialize() {
        // do NOT modify `initialized`
        long current = System.currentTimeMillis();
        if (aligned) {
            while (System.currentTimeMillis() % (1000 * alignment) != 0) {
                continue;
            }
        }
        this.start = System.currentTimeMillis();
    }

    public Integer next() {
        long now = System.currentTimeMillis();
        if (!initialized) {
            initialize();
            initialized = true;
            lastUpdateTS = now;
        }

        if (now - lastUpdateTS > 10000) {
            lastUpdateTS = now;
            //            System.out.println(
            //                    "Top 10 counts: "
            //                            + Arrays.stream(counts)
            //                                    .mapToDouble(i -> i / (double) total)
            //                                    .boxed()
            //                                    .collect(toList()));
            Arrays.fill(counts, 0);
            total = 0;
        }

        int index = nextIndex();
        int originalKey = distributions.get(index).sample() - 1;
        int key = mappings.get(index).get(originalKey);
        if (originalKey < NUM_TOP_KEYS) {
            counts[originalKey]++;
        }
        total++;
        return key;
    }
}
