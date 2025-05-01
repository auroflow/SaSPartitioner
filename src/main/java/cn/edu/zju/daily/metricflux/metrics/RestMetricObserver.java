package cn.edu.zju.daily.metricflux.metrics;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.json.*;

@Slf4j
public class RestMetricObserver implements Runnable {

    private final String jmAddr;
    private int failCount = 0;

    public RestMetricObserver(String jobManagerHost, int jobManagerPort) {
        String jmAddr = jobManagerHost + ":" + jobManagerPort;
        if (!jmAddr.startsWith("http")) {
            jmAddr = "http://" + jmAddr;
        }
        if (jmAddr.endsWith("/")) {
            jmAddr = jmAddr.substring(0, jmAddr.length() - 1);
        }
        if (!jmAddr.endsWith("/v1")) {
            jmAddr += "/v1";
        }
        this.jmAddr = jmAddr;
    }

    public String getRunningJobId() throws Exception {
        String url = jmAddr + "/jobs/overview";
        String json = httpGet(url);
        JSONObject root = new JSONObject(json);
        JSONArray jobs = root.getJSONArray("jobs");
        String runningId = null;
        for (int i = 0; i < jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            if ("RUNNING".equals(job.getString("state"))) {
                if (runningId != null)
                    throw new RuntimeException("More than one RUNNING job found.");
                runningId = job.get("jid").toString();
            }
        }
        if (runningId == null) throw new RuntimeException("No RUNNING job found.");
        return runningId;
    }

    public void getPartitionerIdleRatio(String jobId) throws Exception {
        // Get vertices
        String url = String.format("%s/jobs/%s", jmAddr, jobId);
        String jobJson = httpGet(url);
        JSONObject jobObj = new JSONObject(jobJson);
        JSONArray vertices = jobObj.getJSONArray("vertices");

        List<VertexInfo> partitioners = new ArrayList<>();
        for (int i = 0; i < vertices.length(); i++) {
            JSONObject v = vertices.getJSONObject(i);
            String name = v.getString("name");
            if (name.toLowerCase().contains("partitioner")) {
                if (v.getInt("parallelism") != 1) {
                    LOG.warn("Partitioner parallelism is not 1.");
                } else {
                    partitioners.add(
                            new VertexInfo(v.get("id").toString(), name, v.getInt("parallelism")));
                }
            }
        }
        if (partitioners.isEmpty()) {
            throw new RuntimeException("No partitioner found.");
        }
        if (partitioners.size() != 1) {
            throw new RuntimeException("More than one partitioner found.");
        }

        // Get busyTimeMsPerSecond metric for each partitioner vertex (has aggregation in response)
        double totalIdleRatio = 0;
        int count = 0;
        for (VertexInfo vi : partitioners) {
            String mURL =
                    String.format(
                            "%s/jobs/%s/vertices/%s/subtasks/metrics?get=idleTimeMsPerSecond",
                            jmAddr, jobId, vi.id);
            String metricJson = httpGet(mURL);
            JSONArray metrics = new JSONArray(metricJson);
            double idleAvg = -1;
            for (int i = 0; i < metrics.length(); i++) {
                JSONObject m = metrics.getJSONObject(i);
                if ("idleTimeMsPerSecond".equals(m.getString("id")) && m.has("avg")) {
                    idleAvg = m.optDouble("avg", -1);
                    break;
                }
            }
            if (idleAvg >= 0) {
                // Idle ratio = 1 - busy%
                totalIdleRatio += idleAvg;
                count++;
            }
        }

        if (count != 1) {
            throw new RuntimeException("No idleTimeMsPerSecond found.");
        } else {
            LOG.info("Partitioner idle ratio: {}", totalIdleRatio);
        }
    }

    public void getTMMemory() throws Exception {
        String[] metrics =
                new String[] {
                    "Status.JVM.Memory.Heap.Used",
                    "Status.JVM.Memory.NonHeap.Used",
                    "Status.JVM.Memory.Metaspace.Used",
                    "Status.JVM.Memory.Direct.MemoryUsed",
                    "Status.JVM.Memory.Mapped.MemoryUsed",
                    "Status.Flink.Memory.Managed.Used",
                    "Status.JVM.CPU.Load"
                };
        String url = jmAddr + "/taskmanagers/metrics?get=" + String.join(",", metrics) + "&agg=avg";
        String metricJson = httpGet(url);
        JSONArray arr = new JSONArray(metricJson);
        Map<String, Double> m = new LinkedHashMap<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject o = arr.getJSONObject(i);
            m.put(o.getString("id"), o.optDouble("avg", 0.0));
        }
        for (String k : metrics) {
            double v = m.getOrDefault(k, -1.0);
            if (v == -1.0) {
                throw new RuntimeException("Cannot find metric " + k);
            }
            LOG.info("[{}]: {}", k, v);
        }
    }

    public void getCombinerIdleRatios(String jobId) throws Exception {
        // Get vertices
        String url = String.format("%s/jobs/%s", jmAddr, jobId);
        String jobJson = httpGet(url);
        JSONObject jobObj = new JSONObject(jobJson);
        JSONArray vertices = jobObj.getJSONArray("vertices");

        List<VertexInfo> combiners = new ArrayList<>();
        for (int i = 0; i < vertices.length(); i++) {
            JSONObject v = vertices.getJSONObject(i);
            String name = v.getString("name");
            if (name.toLowerCase().contains("aggregate")) {
                combiners.add(
                        new VertexInfo(v.get("id").toString(), name, v.getInt("parallelism")));
            }
        }
        if (combiners.isEmpty()) {
            throw new RuntimeException("No combiner found.");
        }
        if (combiners.size() != 1) {
            throw new RuntimeException("More than one combiner found.");
        }

        // Get busyTimeMsPerSecond metric for each partitioner vertex (has aggregation in response)
        for (VertexInfo vi : combiners) {
            List<Integer> idleRatios = new ArrayList<>();

            List<String> queries = new ArrayList<>();
            for (int i = 0; i < vi.parallelism; i++) {
                queries.add(i + ".idleTimeMsPerSecond");
            }
            String queryStr = String.join(",", queries);

            String mURL =
                    String.format(
                            "%s/jobs/%s/vertices/%s/metrics?get=%s",
                            jmAddr, jobId, vi.id, queryStr);

            String metricJson = httpGet(mURL);
            JSONArray metrics = new JSONArray(metricJson);
            for (int par = 0; par < vi.parallelism; par++) {
                for (int i = 0; i < vi.parallelism; i++) {
                    JSONObject m = metrics.getJSONObject(i);
                    if ((par + ".idleTimeMsPerSecond").equals(m.getString("id"))) {
                        idleRatios.add(m.optInt("value"));
                        break;
                    }
                }
            }

            if (idleRatios.size() != vi.parallelism) {
                throw new RuntimeException(
                        "Not enough idleTimeMsPerSecond found: " + idleRatios.size());
            } else {
                LOG.info("Combiner idle ratio: {}", idleRatios);
            }
        }
    }

    private String httpGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);
        conn.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) response.append(line);
        in.close();
        return response.toString();
    }

    @Override
    public void run() {
        runInternal();
        System.out.println("Exiting RestMetricObserver.");
        LOG.info("Exiting RestMetricObserver.");
    }

    private void runInternal() {

        // Get running job ID
        String jobId = null;
        int tryCount = 0;
        final int maxTries = 10;
        while (tryCount < maxTries) {
            try {
                jobId = getRunningJobId();
                break;
            } catch (Exception e) {
                tryCount++;
                LOG.error("Try {}/{}: Failed to get running job ID.", tryCount, maxTries, e);
                sleep(10);
            }
        }
        if (jobId == null) {
            LOG.error("Cannot find RUNNING job after {} tries. Exit.", maxTries);
            return;
        }

        while (true) {
            try {
                // Check if job still running
                String url = jmAddr + "/jobs/overview";
                JSONObject root = new JSONObject(httpGet(url));
                JSONArray jobs = root.getJSONArray("jobs");
                boolean found = false;
                for (int i = 0; i < jobs.length(); i++) {
                    JSONObject job = jobs.getJSONObject(i);
                    if (jobId.equals(job.get("jid").toString())) {
                        found = "RUNNING".equals(job.getString("state"));
                        break;
                    }
                }
                if (!found) {
                    LOG.info("Job {} is not RUNNING. Exit.", jobId);
                    return;
                }
                getPartitionerIdleRatio(jobId);
                getTMMemory();
                getCombinerIdleRatios(jobId);
                failCount = 0;
                sleep(1);
            } catch (Exception e) {
                failCount++;
                if (failCount > 3) {
                    LOG.error("Failed more than 3 times. Exit.");
                    LOG.error("FailCount {}/3: Failed to get running job ID.", failCount, e);
                    return;
                }
                sleep(1);
            }
        }
    }

    private void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignored) {
        }
    }

    private static class VertexInfo {
        public final String id;
        public final String name;
        public final int parallelism;

        public VertexInfo(String id, String name, int parallelism) {
            this.id = id;
            this.name = name;
            this.parallelism = parallelism;
        }
    }

    public static void main(String[] args) {
        RestMetricObserver observer = new RestMetricObserver("node11", 4978);
        observer.run();
    }
}
