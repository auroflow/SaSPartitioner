package cn.edu.zju.daily.metricflux.core.data;

import java.io.Serializable;

public interface Record<K> extends Serializable {

    K getKey();

    long getTs();

    boolean isHot();

    long getRouteVersion();

    void setKey(K key);

    void setTs(long ts);

    void setHot(boolean hot);

    void setRouteVersion(long routeVersion);
}
