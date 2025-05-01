package cn.edu.zju.daily.metricflux.core.operator;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;

/**
 * Flink uses MurmurHash to determine the actual partition for a key (i.e. hash(key) % parallelism).
 * This class reverts this process, so that the partition of a key is simply determined by key %
 * parallelism.
 */
public class UniformKeyExtractor<R> implements KeySelector<Tuple2<Integer, R>, Integer> {

    private final Map<Integer, Integer> nodeIdMap;

    static Map<Integer, Integer> getNodeIdMap(int parallelism) {
        Map<Integer, Integer> nodeIdMap = new HashMap<>();
        int count = 0;
        for (int key = 0; count < parallelism; key++) {
            int nodeId = MathUtils.murmurHash(key) % parallelism;
            if (!nodeIdMap.containsKey(nodeId)) {
                nodeIdMap.put(nodeId, key);
                count++;
            }
        }
        return nodeIdMap;
    }

    public UniformKeyExtractor(int numKeys) {
        nodeIdMap = getNodeIdMap(numKeys);
    }

    @Override
    public Integer getKey(Tuple2<Integer, R> value) throws Exception {
        Integer partition = nodeIdMap.get(value.f0);
        if (partition == null) {
            throw new RuntimeException(
                    "partition ID " + value.f0 + " is greater than combiner parallelism.");
        }
        return partition;
    }
}
