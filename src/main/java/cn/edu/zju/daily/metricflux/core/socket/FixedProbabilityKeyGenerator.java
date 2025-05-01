package cn.edu.zju.daily.metricflux.core.socket;

import static java.util.stream.Collectors.toList;

import java.util.*;

public class FixedProbabilityKeyGenerator implements Iterator<Integer> {

    private final List<List<Integer>> maps; // for each list: list[x] = the appearing ratio of x
    private final long shiftInterval; // seconds
    private long alignment = 30; // seconds
    private long start;
    private final boolean aligned; // aligned to ts % shiftInterval == 0
    private boolean initialized = false;
    private final Random random;

    public FixedProbabilityKeyGenerator(Map<Integer, Integer> probability) {
        this(List.of(probability), Long.MAX_VALUE, 0);
    }

    public FixedProbabilityKeyGenerator(
            List<Map<Integer, Integer>> probabilities, long shiftInterval, long alignment) {
        this.maps =
                probabilities.stream()
                        .map(
                                probability -> {
                                    List<Integer> list = new ArrayList<>();
                                    for (Map.Entry<Integer, Integer> entry :
                                            probability.entrySet()) {
                                        for (int i = 0; i < entry.getValue(); i++) {
                                            list.add(entry.getKey());
                                        }
                                    }
                                    return list;
                                })
                        .collect(toList());
        random = new Random(38324);
        this.shiftInterval = shiftInterval;
        this.start = System.currentTimeMillis();
        this.aligned = alignment > 0;
        this.alignment = alignment;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    private int nextIndex() {
        long elapsed = System.currentTimeMillis() - start;
        return (int) Math.floorMod(elapsed / (1000 * shiftInterval), maps.size());
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
        if (!initialized) {
            initialize();
            initialized = true;
        }
        int index = nextIndex();
        List<Integer> map = maps.get(index);
        return map.get(random.nextInt(map.size()));
    }
}
