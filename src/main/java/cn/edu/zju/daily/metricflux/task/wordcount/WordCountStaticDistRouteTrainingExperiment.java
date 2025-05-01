package cn.edu.zju.daily.metricflux.task.wordcount;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.wordcount.socket.TextSocketServer;
import cn.edu.zju.daily.metricflux.task.wordcount.socket.TextSuppliers;
import cn.edu.zju.daily.metricflux.task.wordcount.source.TimestampedWordCountSocketRecordFunction;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import cn.edu.zju.daily.metricflux.utils.rate.AdaptiveRateLimiter;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Train a partitioner with an external policy server. */
public class WordCountStaticDistRouteTrainingExperiment {

    private static final String DEFAULT_PARAM_PATH =
            "/home/user/code/saspartitioner/src/main/resources/params.yaml";

    private final Parameters params;
    private TimestampedSocketRecordFunction sourceFunction;
    private RateLimiter rateLimiter;
    private TextSocketServer server;
    private final boolean trackLatency = false;

    public WordCountStaticDistRouteTrainingExperiment() {
        this(DEFAULT_PARAM_PATH);
    }

    public WordCountStaticDistRouteTrainingExperiment(String paramPath) {
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
        sourceFunction = new TimestampedWordCountSocketRecordFunction(host, port);

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
                new TextSocketServer(
                        hosts.get(0), ports.get(0), TextSuppliers.getSupplier(params), rateLimiter);
        server.startAsync();
    }

    private void execute() throws Exception {
        WordCountExecutor.execute(
                params,
                sourceFunction,
                params.getLearningPartitioner(),
                this.getClass().getSimpleName());
    }

    public void stopSocketServer() {
        server.stop();
    }

    public static void main(String[] args) {
        WordCountStaticDistRouteTrainingExperiment exp =
                new WordCountStaticDistRouteTrainingExperiment();
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
