package cn.edu.zju.daily.metricflux.core.data;

import java.io.Serializable;

public interface PartialResult<K> extends Serializable {

    long getTs();

    K getKey();

    boolean isHot();

    FinalResult<K> toFinal();
}
