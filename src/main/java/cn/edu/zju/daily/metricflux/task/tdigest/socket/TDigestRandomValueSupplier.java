package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.SplittableRandom;

public class TDigestRandomValueSupplier extends TDigestValueSupplier {

    private final int valueSize;
    private final byte[] bytes = new byte[Double.BYTES];

    public TDigestRandomValueSupplier(int valueSize) {
        this.valueSize = valueSize;
    }

    private final SplittableRandom random = new SplittableRandom();

    @Override
    protected void trySupply(DataOutputStream out) throws IOException {
        out.writeInt(valueSize);
        for (int i = 0; i < valueSize; i++) {
            out.writeDouble(random.nextDouble());
        }
    }
}
