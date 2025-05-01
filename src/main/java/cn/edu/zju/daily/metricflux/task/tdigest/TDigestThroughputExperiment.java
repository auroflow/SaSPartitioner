package cn.edu.zju.daily.metricflux.task.tdigest;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.*;
import cn.edu.zju.daily.metricflux.task.tdigest.source.TimestampedTDigestSocketRecordFunction;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.VariableRateLimiter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class TDigestThroughputExperiment {
    interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

    private static final String DEFAULT_PARAM_PATH =
            "/home/user/code/saspartitioner/src/main/resources/params.yaml";

    private final Parameters params;
    private TimestampedSocketRecordFunction sourceFunction;
    private VariableRateLimiter rateLimiter;
    private TDigestSocketServer server;
    private static final boolean TRACK_LATENCY = false;

    public TDigestThroughputExperiment() {
        params = Parameters.load(DEFAULT_PARAM_PATH, false);
    }

    private void startSocketServer() throws IOException {

        List<String> hosts = List.of(InetAddress.getLocalHost().getHostName());
        System.out.println("Host: " + hosts.get(0));
        List<Integer> ports = params.getInputSocketPorts();
        if (ports.size() != 1) {
            System.out.println("Warning: ports.size() != 1. The first port will be used.");
        }
        sourceFunction =
                new TimestampedTDigestSocketRecordFunction(hosts.get(0), ports.get(0), 100);

        rateLimiter =
                new VariableRateLimiter(
                        params.getWarmupRates(),
                        params.getWarmupIntervalsSeconds(),
                        params.getInitialRate(),
                        params.getRateStep(),
                        params.getRateIntervalSeconds(),
                        params.getMaxRate(),
                        params.getJobManagerHost(),
                        params.getJobManagerPort());

        server = TDigestSuppliers.getSocketServer(hosts, ports, params, rateLimiter);
        rateLimiter.setSocketServer(server);
        server.startAsync();
    }

    private void execute() throws Exception {
        TDigestExecutor.execute(
                params, sourceFunction, params.getPartitioner(), this.getClass().getSimpleName());
    }

    public void stopSocketServer() {
        server.stop();
    }

    private void sleep(long seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TDigestThroughputExperiment exp = new TDigestThroughputExperiment();
        try {
            exp.startSocketServer();
            exp.execute();
        } catch (ExecutionException e) {
            System.out.println("Job exited. Backpressure can cause this.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            exp.stopSocketServer();
        }
    }
}
