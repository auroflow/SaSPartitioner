package cn.edu.zju.daily.metricflux.core.operator;

import static smile.math.MathEx.*;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LatencyTracker<F> extends ProcessFunction<F, F> {

    private ArrayList<Long> latencies;
    private long lastEventTime;
    private final OutputTag<Tuple2<Integer, ArrayList<Long>>> outputTag;

    public LatencyTracker(OutputTag<Tuple2<Integer, ArrayList<Long>>> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        latencies = new ArrayList<>();
    }

    @Override
    public void processElement(F value, ProcessFunction<F, F>.Context ctx, Collector<F> out)
            throws Exception {
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        long eventTime = ctx.timestamp();
        long delay = currentProcessingTime - eventTime;

        if (lastEventTime == eventTime) {
            latencies.add(delay);
        } else if (lastEventTime < eventTime) {
            latencies.add(delay);

            // a new window has begun
            lastEventTime = eventTime;
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            ctx.output(outputTag, Tuple2.of(subtaskIndex, latencies));
            latencies.clear();
        }
        // out-of-order events are ignored

        out.collect(value);
    }

    // assume arr is sorted
    double percentile(double[] arr, double p) {
        return arr[(int) Math.floor(arr.length * p)];
    }
}
