package cn.edu.zju.daily.metricflux.core.data;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractRecord<K> implements Record<K> {

    private K key;
    private long ts;
    private boolean hot = true;
    private long routeVersion = 0;

    public AbstractRecord() {}

    public AbstractRecord(K key, long ts) {
        this.key = key;
        this.ts = ts;
    }
}
