package cn.edu.zju.daily.metricflux.core.operator;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CombinerProcessWindowFunction<K, P extends PartialResult<K>, F extends FinalResult<K>>
        extends ProcessWindowFunction<Map<K, P>, P, Integer, TimeWindow> {

    OutputTag<F> nonHotTag;

    /**
     * Constructor.
     *
     * @param nonHotTag the output tag for non-hot keys. If null, non-hot keys will be treated as
     *     hot keys (i.e. sent to the main output).
     */
    public CombinerProcessWindowFunction(OutputTag<F> nonHotTag) {
        this.nonHotTag = nonHotTag;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(
            Integer integer,
            ProcessWindowFunction<Map<K, P>, P, Integer, TimeWindow>.Context ctx,
            Iterable<Map<K, P>> elements,
            Collector<P> out)
            throws Exception {

        for (Map<K, P> value : elements) {
            for (Map.Entry<K, P> entry : value.entrySet()) {
                if (entry.getValue().isHot() || nonHotTag == null) {
                    out.collect(entry.getValue());
                } else {
                    ctx.output(nonHotTag, (F) entry.getValue().toFinal());
                }
            }
        }
    }
}
