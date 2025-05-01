package cn.edu.zju.daily.metricflux.core;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.core.operator.*;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

public class KeySplittingPipeline<
        K, R extends Record<K>, P extends PartialResult<K>, F extends FinalResult<K>> {

    private final Parameters params;
    private final ProcessFunction<R, Tuple2<Integer, R>> partitioner;
    private final Combiner<K, R, P> combiner;
    private final Reducer<K, P, F> reducer;
    private final KeySelector<P, Integer> reduceKeyExtractor;
    private final TypeHint<K> keyTypeHint;
    private final TypeHint<P> partialResultTypeHint;
    private final TypeHint<F> finalResultTypeHint;
    private final boolean trackLatency;

    public KeySplittingPipeline(
            Parameters params,
            ProcessFunction<R, Tuple2<Integer, R>> partitioner,
            Combiner<K, R, P> combiner,
            Reducer<K, P, F> reducer,
            KeySelector<P, Integer> reducerKeyExtractor,
            TypeHint<K> keyTypeHint,
            TypeHint<P> partialResultTypeHint,
            TypeHint<F> finalResultTypeHint,
            boolean trackLatency) {
        this.params = params;
        this.partitioner = partitioner;
        this.combiner = combiner;
        this.reducer = reducer;
        this.reduceKeyExtractor = reducerKeyExtractor;
        this.keyTypeHint = keyTypeHint;
        this.partialResultTypeHint = partialResultTypeHint;
        this.finalResultTypeHint = finalResultTypeHint;
        this.trackLatency = trackLatency;
    }

    public static <T> SinkFunction<T> getSink(Parameters params, boolean discard) {
        if (discard) {
            return new DiscardingSink<T>();
        }
        // current timestamp, in YYYYMMDD-HHmm format
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").format(LocalDateTime.now());
        String path = params.getHdfsOutputDirectory();
        if (!path.endsWith("/")) {
            path += "/";
        }
        path += timestamp;

        return StreamingFileSink.forRowFormat(
                        new org.apache.flink.core.fs.Path(path),
                        new SimpleStringEncoder<T>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(60))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build();
    }

    public DataStream<?> transform(SingleOutputStreamOperator<R> inputStream) {

        int combinerParallelism = params.getCombinerParallelism();
        int reducerParallelism = params.getReducerParallelism();
        int slide = params.getWindowSlide();
        int size = params.getWindowSize();
        int numKeys = params.getNumKeys();
        Duration windowSlide = Duration.ofMillis(slide);
        Duration windowSize = Duration.ofMillis(size);
        OutputTag<F> nonHotTag =
                new OutputTag<F>("non-hot", TypeInformation.of(finalResultTypeHint));
        OutputTag<Tuple2<Integer, ArrayList<Long>>> latencyTag =
                new OutputTag<>(
                        "latency",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, ArrayList<Long>>>() {}));
        CombinerProcessWindowFunction<K, P, F> combinerProcessWindowFunction =
                new CombinerProcessWindowFunction<>(nonHotTag);

        // Step 1. Partition
        // each output element is a pair of (partitionId, record)
        SingleOutputStreamOperator<Tuple2<Integer, R>> partitioned;
        partitioned =
                inputStream
                        .process(partitioner)
                        .name("partitioner")
                        .slotSharingGroup("source_partitioner")
                        .uid("partitioner");

        // If partitioner only, return the partitioned stream
        if (params.isPartitionerOnly()) {
            return partitioned;
        }

        // Step 2. Combine into partial results
        // each element is a pair of (key, partialResult), partitioned by partitionId
        SingleOutputStreamOperator<P> hotPartialResult =
                partitioned
                        .keyBy(new UniformKeyExtractor<>(combinerParallelism))
                        .window(SlidingEventTimeWindows.of(windowSize, windowSlide))
                        .aggregate(combiner, combinerProcessWindowFunction)
                        .returns(TypeInformation.of(partialResultTypeHint))
                        .setParallelism(combinerParallelism)
                        .setMaxParallelism(combinerParallelism)
                        .slotSharingGroup("combiner")
                        .name("aggregate")
                        .uid("aggregate");

        // Step 4. Reduce partial results
        SingleOutputStreamOperator<F> hotResult =
                hotPartialResult
                        .keyBy(reduceKeyExtractor)
                        .window(TumblingEventTimeWindows.of(windowSlide))
                        .aggregate(reducer)
                        .returns(TypeInformation.of(finalResultTypeHint))
                        .setParallelism(reducerParallelism)
                        .setMaxParallelism(reducerParallelism)
                        .slotSharingGroup("reducer")
                        .name("reduce")
                        .uid("reduce");

        SideOutputDataStream<F> nonHotResult = hotPartialResult.getSideOutput(nonHotTag);

        // Step 5. Track latency (optional)
        if (trackLatency) {
            SingleOutputStreamOperator<F> tracked =
                    hotResult
                            .union(nonHotResult)
                            .process(new LatencyTracker<>(latencyTag))
                            .setParallelism(combinerParallelism)
                            .setMaxParallelism(combinerParallelism)
                            .slotSharingGroup("combiner")
                            .name("tracker");

            tracked.getSideOutput(latencyTag)
                    .windowAll(TumblingEventTimeWindows.of(windowSlide))
                    .process(new LatencyReporter())
                    .setParallelism(1)
                    .setMaxParallelism(1)
                    .name("latency-reporter")
                    .uid("latency-reporter")
                    .slotSharingGroup("latency-reporter");

            return tracked;
        } else {
            return hotResult.union(nonHotResult);
        }
    }
}
