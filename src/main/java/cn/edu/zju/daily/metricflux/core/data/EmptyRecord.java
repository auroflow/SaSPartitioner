package cn.edu.zju.daily.metricflux.core.data;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmptyRecord<K> implements Record<K> {

    private long routeVersion;

    @Override
    public K getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKey(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTs(long ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHot(boolean hot) {
        throw new UnsupportedOperationException();
    }
}
