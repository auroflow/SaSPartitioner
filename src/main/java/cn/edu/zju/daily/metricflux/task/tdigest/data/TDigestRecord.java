package cn.edu.zju.daily.metricflux.task.tdigest.data;

import cn.edu.zju.daily.metricflux.core.data.AbstractRecord;
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TDigestRecord extends AbstractRecord<Integer> {

    private double[] values;

    public TDigestRecord() {}

    public TDigestRecord(int key, long ts, double[] values) {
        super(key, ts);
        this.values = values;
    }

    @Override
    public String toString() {
        return "TDigestRecord{"
                + "key="
                + getKey()
                + ", ts="
                + getTs()
                + ", values="
                + Arrays.toString(values)
                + '}';
    }
}
