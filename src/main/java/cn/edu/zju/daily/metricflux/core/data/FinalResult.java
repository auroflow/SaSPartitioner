package cn.edu.zju.daily.metricflux.core.data;

import java.io.Serializable;

public interface FinalResult<K> extends Serializable {

    long getTs();

    K getKey();
}
