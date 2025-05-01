package cn.edu.zju.daily.metricflux.core.operator;

import static smile.math.MathEx.*;

import cn.edu.zju.daily.metricflux.partitioner.metrics.HistogramStatistics;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyReporter
        extends ProcessAllWindowFunction<Tuple2<Integer, ArrayList<Long>>, Object, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(LatencyReporter.class);

    // assume arr is sorted
    int percentile(int[] arr, double p) {
        return arr[(int) Math.floor(arr.length * p)];
    }

    @Override
    public void process(
            ProcessAllWindowFunction<Tuple2<Integer, ArrayList<Long>>, Object, TimeWindow>.Context
                    context,
            Iterable<Tuple2<Integer, ArrayList<Long>>> elements,
            Collector<Object> out)
            throws Exception {
        ArrayList<Long> latencies = new ArrayList<>();
        for (Tuple2<Integer, ArrayList<Long>> element : elements) {
            latencies.addAll(element.f1);
            log(element.f1, "partition " + element.f0.toString());
        }
        log(latencies, "partition all");
    }

    private void log(ArrayList<Long> latencies, String name) {
        int[] arr = latencies.stream().mapToInt(Long::intValue).sorted().toArray();
        if (latencies.size() >= 2) {
            HistogramStatistics stats =
                    new HistogramStatistics(
                            min(arr),
                            max(arr),
                            mean(arr),
                            sd(arr),
                            percentile(arr, 0.5),
                            percentile(arr, 0.99));
            LOG.info("Latency statistics ({}): {}", name, stats);
        }
    }
}
