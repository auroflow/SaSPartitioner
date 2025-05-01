package cn.edu.zju.daily.metricflux.task.wordcount;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.wordcount.socket.TextSocketServer;
import cn.edu.zju.daily.metricflux.task.wordcount.socket.TextSuppliers;
import cn.edu.zju.daily.metricflux.task.wordcount.source.TimestampedWordCountSocketRecordFunction;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.VariableRateLimiter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Test a pipeline with an increasing source rate until backpressure is observed.
 *
 * <p>The method is to gradually increase source speed at fixed intervals. In each interval, if the
 * actual source speed is slower than the desired source speed by 5%, the system reaches
 * back-pressured state, and the current source speed is returned as the maximum throughput the
 * system can handle.
 */
public class WordCountThroughputExperimentV2 {

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

    private final Parameters params;
    private TimestampedSocketRecordFunction sourceFunction;
    private VariableRateLimiter rateLimiter;
    private TextSocketServer server;
    private static final boolean TRACK_LATENCY = false;

    public WordCountThroughputExperimentV2() {
        params =
                Parameters.load(
                        "/home/user/code/saspartitioner/src/main/resources/params.yaml", false);
    }

    private void startSocketServer() throws IOException {

        String host = InetAddress.getLocalHost().getHostName();
        int port = params.getInputSocketPorts().get(0);

        int p = params.getCombinerParallelism();

        List<String> hosts = List.of(InetAddress.getLocalHost().getHostName());
        System.out.println("Host: " + hosts.get(0));
        List<Integer> ports = params.getInputSocketPorts();
        if (ports.size() != 1) {
            System.out.println("Warning: ports.size() != 1. The first port will be used.");
        }
        sourceFunction = new TimestampedWordCountSocketRecordFunction(host, port);

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
        server =
                new TextSocketServer(
                        hosts.get(0), ports.get(0), TextSuppliers.getSupplier(params), rateLimiter);
        rateLimiter.setSocketServer(server);
        server.startAsync();
    }

    private void execute() throws Exception {
        WordCountExecutor.execute(
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
        WordCountThroughputExperimentV2 exp = new WordCountThroughputExperimentV2();
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
