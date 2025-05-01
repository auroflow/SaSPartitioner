package cn.edu.zju.daily.metricflux.core.operator;

import cn.edu.zju.daily.metricflux.core.data.Record;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class WorkerReceiver<K, R extends Record<K>>
        extends RichMapFunction<Tuple2<Integer, R>, Tuple2<Integer, R>> {

    @Override
    public Tuple2<Integer, R> map(Tuple2<Integer, R> value) throws Exception {

        return value;
    }
}
