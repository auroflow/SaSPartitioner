package cn.edu.zju.daily.metricflux.task.tdigest.data;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import cn.edu.zju.daily.metricflux.task.tdigest.stats.TDigest;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TDigestState implements PartialResult<Integer>, FinalResult<Integer> {

    private long ts;
    private Integer key;
    private TDigest digest;
    private boolean hot = false;

    public TDigestState(int key) {
        this.key = key;
        this.digest = TDigest.createMergingDigest(100);
    }

    public void count(double[] value) {
        if (value == null) {
            return;
        }
        for (double v : value) {
            this.digest.add(v);
        }
    }

    public void merge(TDigestState other) {
        if (!Objects.equals(this.key, other.key)) {
            throw new IllegalArgumentException("Cannot merge states with different keys");
        }
        this.digest.add(other.digest);
        this.hot = this.hot || other.hot;
    }

    @Override
    public FinalResult<Integer> toFinal() {
        return this;
    }
}
