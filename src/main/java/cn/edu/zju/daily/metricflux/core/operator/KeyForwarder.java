package cn.edu.zju.daily.metricflux.core.operator;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import java.io.Serializable;
import java.util.Map;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyForwarder<K, P extends PartialResult<K>, F extends FinalResult<K>>
        extends ProcessFunction<Map<K, P>, P> implements Serializable {

    OutputTag<F> nonHotTag;

    /**
     * Constructor.
     *
     * @param nonHotTag the output tag for non-hot keys. If null, non-hot keys will be treated as
     *     hot keys (i.e. sent to the main output).
     */
    public KeyForwarder(OutputTag<F> nonHotTag) {
        this.nonHotTag = nonHotTag;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(
            Map<K, P> value, ProcessFunction<Map<K, P>, P>.Context ctx, Collector<P> out)
            throws Exception {

        for (Map.Entry<K, P> entry : value.entrySet()) {
            if (entry.getValue().isHot() || nonHotTag == null) {
                out.collect(entry.getValue());
            } else {
                ctx.output(nonHotTag, (F) entry.getValue().toFinal());
            }
        }
    }
}
