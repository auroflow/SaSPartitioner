package cn.edu.zju.daily.metricflux.partitioner.learning;

import java.io.Closeable;
import java.util.Map;

public interface RLPolicyClient<OBS, ACT> extends Closeable {

    double[] getNextAction(double[] obs, double reward, Map<String, ?> info, boolean done);
}
