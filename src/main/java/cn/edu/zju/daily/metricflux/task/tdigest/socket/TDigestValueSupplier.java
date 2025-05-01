package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class TDigestValueSupplier {

    public void supply(DataOutputStream out) {
        try {
            trySupply(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void trySupply(DataOutputStream out) throws IOException;
}
