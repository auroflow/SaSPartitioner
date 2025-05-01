package cn.edu.zju.daily.metricflux.task.tdigest;

import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestRecord;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestDatasetSupplier;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestRandomValueSupplier;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestSocketServer;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestValueSupplier;
import cn.edu.zju.daily.metricflux.task.tdigest.source.TimestampedTDigestSocketRecordFunction;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.AdaptiveRateLimiter;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TDigestStaticDistRouteTrainingExperiment {

    private static final String DEFAULT_PARAM_PATH =
            "/home/user/code/saspartitioner/src/main/resources/params.yaml";
    private static final int VALUE_SIZE = 100;

    private final Parameters params;
    private TimestampedSocketRecordFunction<Integer, TDigestRecord> sourceFunction;
    private RateLimiter rateLimiter;
    private TDigestSocketServer server;
    private final boolean trackLatency = false;

    public TDigestStaticDistRouteTrainingExperiment() {
        this(DEFAULT_PARAM_PATH);
    }

    public TDigestStaticDistRouteTrainingExperiment(String paramPath) {
        params = Parameters.load(paramPath, false);
    }

    private void startSocketServer() throws IOException {

        String host = InetAddress.getLocalHost().getHostName();
        int port = params.getInputSocketPorts().get(0);

        List<String> hosts = List.of(InetAddress.getLocalHost().getHostName());
        System.out.println("Host: " + hosts.get(0));
        List<Integer> ports = params.getInputSocketPorts();
        if (ports.size() != 1) {
            System.out.println("Warning: ports.size() != 1. The first port will be used.");
        }
        sourceFunction = new TimestampedTDigestSocketRecordFunction(host, port, VALUE_SIZE);

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

        rateLimiter =
                new AdaptiveRateLimiter(
                        params.getDataRateForLearning(),
                        params.getRateStep(),
                        params.getJobManagerHost(),
                        params.getJobManagerPort(),
                        params.getFirstAdaptiveDurationSec(),
                        params.getNextAdaptiveDurationSec(),
                        params.isDoTest(),
                        params.isTestOnlyOnce());

        server =
                new TDigestSocketServer(
                        hosts.get(0),
                        ports.get(0),
                        keyGenerator,
                        valueSupplier,
                        rateLimiter,
                        valueBufferCapacity);
        server.startAsync();
    }

    private void execute() throws Exception {
        TDigestExecutor.execute(
                params,
                sourceFunction,
                params.getLearningPartitioner(),
                this.getClass().getSimpleName());
    }

    public void stopSocketServer() {
        server.stop();
    }

    public static void main(String[] args) {
        TDigestStaticDistRouteTrainingExperiment exp =
                new TDigestStaticDistRouteTrainingExperiment();
        try {
            exp.startSocketServer();
            exp.execute();
        } catch (ExecutionException e) {
            System.out.println("Job exited.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            exp.stopSocketServer();
        }
    }
}
