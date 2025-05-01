package cn.edu.zju.daily.metricflux.core.operator;

import static java.util.stream.Collectors.toList;

import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.MathUtils;

public class UniformReduceKeyExtractor<K, P extends PartialResult<K>>
        implements KeySelector<P, Integer> {

    private static final int NOT_FOUND = -1;

    private final Map<K, Integer> partitionKeyMap; // data key to partition key

    // For testing.
    Map<K, Integer> getPartitionKeyMap() {
        return Collections.unmodifiableMap(partitionKeyMap);
    }

    /**
     * Constructor.
     *
     * @param reducerParallelism the parallelism of the reducer
     * @param fanouts the fanouts of each hot key
     */
    public UniformReduceKeyExtractor(
            int reducerParallelism, List<K> hotKeys, List<Integer> fanouts) {

        if (hotKeys.size() != fanouts.size()) {
            throw new IllegalArgumentException("hotKeys and fanouts must have the same size");
        }

        Map<K, Integer> keyToPartition = new HashMap<>(); // data key to partition

        int[] counts = new int[reducerParallelism];

        List<Integer> hotKeyIndexes = IntStream.range(0, hotKeys.size()).boxed().collect(toList());
        hotKeyIndexes.sort((i, j) -> Integer.compare(fanouts.get(j), fanouts.get(i)));

        for (int i : hotKeyIndexes) {
            K key = hotKeys.get(i);
            int fanout = fanouts.get(i);

            int smallest = 0;
            for (int j = 1; j < counts.length; j++) {
                if (counts[j] < counts[smallest]) {
                    smallest = j;
                }
            }
            keyToPartition.put(key, smallest);
            counts[smallest] += fanout;
        }

        int partitionKey = 0;
        partitionKeyMap = new HashMap<>();
        while (partitionKeyMap.size() < hotKeys.size()) {
            int nodeId = MathUtils.murmurHash(partitionKey) % reducerParallelism;
            for (K key : hotKeys) {
                if (!partitionKeyMap.containsKey(key) && nodeId == keyToPartition.get(key)) {
                    partitionKeyMap.put(key, partitionKey);
                    break;
                }
            }
            partitionKey++;
        }
    }

    @Override
    public Integer getKey(P value) throws Exception {
        // may have problems if K is not Integer
        K key = value.getKey();
        int partitionKey = partitionKeyMap.getOrDefault(key, NOT_FOUND);
        if (partitionKey == NOT_FOUND) {
            throw new RuntimeException(
                    "key " + key + " is not a hot key and has not been assigned a node ID.");
        }
        return partitionKey;
    }
}
