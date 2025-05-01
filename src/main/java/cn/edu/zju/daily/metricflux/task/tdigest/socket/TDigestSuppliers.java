package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import cn.edu.zju.daily.metricflux.core.socket.DriftingHotKeyGenerator;
import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TDigestSuppliers {

    private static final int VALUE_SIZE = 100;

    public static TDigestSocketServer getSocketServer(
            List<String> hosts, List<Integer> ports, Parameters params, RateLimiter rateLimiter)
            throws IOException {

        String dataset = params.getDataset();
        Iterator<Integer> keyGenerator;
        TDigestValueSupplier valueSupplier;
        int valueBufferCapacity;
        if ("zipf".equals(dataset)) {
            keyGenerator =
                    new ZipfDistributionKeyGenerator(
                            params.getNumKeys(), params.getZipf(), params.isShuffleKey());
            valueSupplier = new TDigestRandomValueSupplier(VALUE_SIZE);
            valueBufferCapacity = Integer.BYTES + (VALUE_SIZE + 1) * Double.BYTES;
        } else if ("zipf-drifting-key".equals(dataset)) {
            keyGenerator =
                    new DriftingHotKeyGenerator(
                            new ZipfDistributionKeyGenerator(params.getNumKeys(), params.getZipf()),
                            params.getNumKeys(), // the last key is the drifting key
                            0.001,
                            0.03,
                            params.getShiftInterval());
            valueSupplier = new TDigestRandomValueSupplier(VALUE_SIZE);
            valueBufferCapacity = Integer.BYTES + (VALUE_SIZE + 1) * Double.BYTES;
        } else if ("zipf-sequence".equals(dataset)) {
            keyGenerator =
                    new ZipfDistributionKeyGenerator(
                            params.getNumKeysSequence(),
                            params.getZipfSequence(),
                            params.getShiftInterval(),
                            params.getShiftAlignment(),
                            params.isShuffleKey());
            valueSupplier = new TDigestRandomValueSupplier(VALUE_SIZE);
            valueBufferCapacity = Integer.BYTES + (VALUE_SIZE + 1) * Double.BYTES;
        } else if ("file".equals(dataset)) {
            TDigestDatasetSupplier supplier = new TDigestDatasetSupplier(params.getDatasetPath());
            keyGenerator = supplier.getKeySupplier();
            valueSupplier = supplier.getValueSupplier();
            valueBufferCapacity = Integer.BYTES + (supplier.getMaxValueSize() + 1) * Double.BYTES;
        } else {
            throw new IllegalArgumentException("Unknown dataset: " + dataset);
        }

        return new TDigestSocketServer(
                hosts.get(0),
                ports.get(0),
                keyGenerator,
                valueSupplier,
                rateLimiter,
                valueBufferCapacity);
    }
}
