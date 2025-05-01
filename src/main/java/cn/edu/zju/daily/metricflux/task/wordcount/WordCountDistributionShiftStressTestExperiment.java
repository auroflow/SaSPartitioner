package cn.edu.zju.daily.metricflux.task.wordcount;

import static cn.edu.zju.daily.metricflux.utils.TimeUtils.rateToDelay;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.metrics.RestMetricObserver;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestSocketServer;
import cn.edu.zju.daily.metricflux.task.tdigest.socket.TDigestSuppliers;
import cn.edu.zju.daily.metricflux.task.wordcount.socket.TextSocketServer;
import cn.edu.zju.daily.metricflux.task.wordcount.source.TimestampedWordCountSocketRecordFunction;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.FixedRateLimiter;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import cn.edu.zju.daily.metricflux.utils.rate.TimedLimiter;
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
public class WordCountDistributionShiftStressTestExperiment {

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

    private final Parameters params;
    private TimestampedSocketRecordFunction sourceFunction;
    private RateLimiter rateLimiter;
    private TextSocketServer server;
    private static final boolean TRACK_LATENCY = false;

    public WordCountDistributionShiftStressTestExperiment() {
        params =
                Parameters.load(
                        "/home/user/code/saspartitioner/src/main/resources/params.yaml", false);
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
        sourceFunction = new TimestampedWordCountSocketRecordFunction(host, port);

        rateLimiter =
                new TimedLimiter(
                        new FixedRateLimiter(rateToDelay(params.getStressTestRate())),
                        params.getStressTestDurationSeconds(),
                        params.getJobManagerHost(),
                        params.getJobManagerPort());

        TDigestSocketServer server =
                TDigestSuppliers.getSocketServer(hosts, ports, params, rateLimiter);
        server.startAsync();
    }

    private void execute() throws Exception {
        WordCountExecutor.execute(
                params,
                sourceFunction,
                params.getStressTestPartitioner(),
                this.getClass().getSimpleName());
    }

    private Thread startMetricObserverAsync() {
        RestMetricObserver observer =
                new RestMetricObserver(params.getJobManagerHost(), params.getJobManagerPort());
        Thread th = new Thread(observer);
        th.start();
        return th;
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
        WordCountDistributionShiftStressTestExperiment exp =
                new WordCountDistributionShiftStressTestExperiment();
        Thread observerThread = null;
        try {
            System.out.println("Starting socket server...");
            exp.startSocketServer();
            System.out.println("Starting metric observer...");
            observerThread = exp.startMetricObserverAsync();
            System.out.println("Executing experiment...");
            exp.execute();
        } catch (ExecutionException e) {
            System.out.println("Job exited. Backpressure can cause this.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            exp.stopSocketServer();
            if (observerThread != null) {
                try {
                    observerThread.join();
                } catch (Exception ignored) {

                }
            }
        }
    }
}
