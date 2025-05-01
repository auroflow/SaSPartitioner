package cn.edu.zju.daily.metricflux.core.operator;

import static cn.edu.zju.daily.metricflux.utils.RoutingTableUtils.getFanouts;
import static cn.edu.zju.daily.metricflux.utils.RoutingTableUtils.getMaskHeuristic;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.Test;

class UniformReduceKeyExtractorTest {

    @Test
    void test() {

        Parameters params = new Parameters();
        params.setDataset("zipf");
        params.setZipf(1);
        params.setCombinerParallelism(64);
        params.setReducerParallelism(1);
        params.setNumHotKeys(10);
        params.setMaskSpreadKeys(0);
        List<Integer> fanouts =
                getFanouts(
                        getMaskHeuristic(params),
                        params.getNumHotKeys(),
                        params.getCombinerParallelism());
        System.out.println("Fanouts: " + fanouts);
        List<Integer> hotKeys = IntStream.range(0, fanouts.size()).boxed().collect(toList());

        UniformReduceKeyExtractor<Integer, PartialResult<Integer>> extractor =
                new UniformReduceKeyExtractor<>(params.getReducerParallelism(), hotKeys, fanouts);

        for (Map.Entry<Integer, Integer> entry : extractor.getPartitionKeyMap().entrySet()) {
            System.out.println(
                    entry.getKey()
                            + " -> "
                            + entry.getValue()
                            + ", partition "
                            + MathUtils.murmurHash(entry.getValue()) % 8);
        }
    }
}
