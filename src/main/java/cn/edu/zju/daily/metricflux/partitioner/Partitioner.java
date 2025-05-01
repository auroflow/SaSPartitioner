package cn.edu.zju.daily.metricflux.partitioner;

import cn.edu.zju.daily.metricflux.core.data.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public abstract class Partitioner<K, R extends Record<K>>
        extends ProcessFunction<R, Tuple2<Integer, R>> {

    private Histogram partitionTime;
    private long count = 0;
    private long nextLogTime = 0;
    private long lastLogTime = 0;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        partitionTime =
                getRuntimeContext()
                        .getMetricGroup()
                        .histogram("partitionTime", new DescriptiveStatisticsHistogram(10000));
    }

    @Override
    public final void processElement(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> collector)
            throws Exception {

        long start = System.nanoTime();
        processElementInternal(r, context, collector);
        partitionTime.update(System.nanoTime() - start);
        count++;

        // Log the partition time every 10 seconds
        if (System.currentTimeMillis() > nextLogTime) {
            // log distribution of partition time
            long thisLogTime = System.currentTimeMillis();
            LOG.info(
                    "Partition time in ns (mean min max stddev): {} {} {} {}",
                    partitionTime.getStatistics().getMean(),
                    partitionTime.getStatistics().getMin(),
                    partitionTime.getStatistics().getMax(),
                    partitionTime.getStatistics().getStdDev());

            // log count
            if (lastLogTime != 0) {
                double rate = count / ((thisLogTime - lastLogTime) / 1000.0);
                LOG.info("Throughput: {} records/s", rate);
            }
            count = 0;

            lastLogTime = thisLogTime;
            nextLogTime = thisLogTime + 10000;
        }
    }

    protected abstract void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context context,
            Collector<Tuple2<Integer, R>> collector)
            throws Exception;

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
