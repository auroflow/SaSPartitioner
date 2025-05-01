package cn.edu.zju.daily.metricflux.partitioner.metrics.monitor;

import cn.edu.zju.daily.metricflux.utils.NetworkUtils;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This one assumes that the job is running in STREAMING mode, meaning all tasks are scheduled in
 * advance.
 */
public class JobInfo implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(JobInfo.class);

    private static final Tuple2<String, String> DEFAULT_TUPLE = new Tuple2<>(null, null);

    @Getter private final String jobName;
    @Getter private final String jobId;

    // "taskName.subtaskId" -> ("host", "taskManagerId")
    private final Map<String, Tuple2<String, String>> subtaskToTM = new HashMap<>();
    // "taskName" -> parallelism
    private final Map<String, Integer> taskToParallelism = new HashMap<>();
    private final Map<String, String> taskToId = new HashMap<>();
    private final Map<String, String> taskIdToName = new HashMap<>();

    public JobInfo(String jobManagerHost, int jobManagerPort) {
        // query Flink REST API for task manager information
        Tuple2<String, String> nameIdPair =
                getFirstRunningJobNameAndId(jobManagerHost, jobManagerPort);
        while (nameIdPair == null) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            nameIdPair = getFirstRunningJobNameAndId(jobManagerHost, jobManagerPort);
        }
        jobName = nameIdPair.f0;
        jobId = nameIdPair.f1;
        getMapping(jobManagerHost, jobManagerPort, nameIdPair.f1);
        LOG.info("Got job info for {}", jobName);
    }

    public String getTaskId(String taskName) {
        for (String task : taskToId.keySet()) {
            if (task.contains(taskName)) {
                return taskToId.get(task);
            }
        }
        return null;
    }

    public String getTaskName(String taskId) {
        return taskIdToName.get(taskId);
    }

    public String getHost(String taskName, int subtaskIndex) {
        // Find mapping task name
        for (String task : taskToId.keySet()) {
            if (task.contains(taskName)) {
                taskName = task;
                break;
            }
        }
        String host = subtaskToTM.get(taskName + "." + subtaskIndex).f0;
        return host;
    }

    public Set<String> getTaskNames() {
        return taskToParallelism.keySet();
    }

    public String getTaskManagerId(String taskName, int subtaskIndex) {
        return subtaskToTM.get(taskName + "." + subtaskIndex).f1;
    }

    public int getTaskParallelism(String taskName) {
        return taskToParallelism.get(taskName);
    }

    private void getMapping(String jobManagerHost, int jobManagerPort, String jobId) {
        String unassigned = "(unassigned)";
        tryGetMapping(jobManagerHost, jobManagerPort, jobId);
        while (subtaskToTM.isEmpty()
                || subtaskToTM.values().stream()
                        .anyMatch(t -> unassigned.equals(t.f0) || unassigned.equals(t.f1))) {
            // If not all assigned, wait and retry
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            subtaskToTM.clear();
            taskToParallelism.clear();
            taskToId.clear();
            taskIdToName.clear();
            tryGetMapping(jobManagerHost, jobManagerPort, jobId);
        }
    }

    private void tryGetMapping(String jobManagerHost, int jobManagerPort, String jobId) {
        // query Flink REST API for mappings
        JSONObject job =
                NetworkUtils.httpGetJson(jobManagerHost, jobManagerPort, "/jobs/" + jobId)
                        .toObject();
        JSONArray vertices = job.getJSONArray("vertices");
        for (int i = 0; i < vertices.length(); i++) {
            JSONObject vertice = vertices.getJSONObject(i);
            String taskName = vertice.getString("name");
            String taskId = vertice.getString("id");
            taskToId.put(taskName, taskId);
            taskIdToName.put(taskId, taskName);
            JSONObject task;
            try {
                task =
                        NetworkUtils.httpGetJson(
                                        jobManagerHost,
                                        jobManagerPort,
                                        "/jobs/" + jobId + "/vertices/" + taskId)
                                .toObject();
            } catch (Exception e) {
                LOG.warn("Failed to get task {}, retrying...", taskName);
                subtaskToTM.clear();
                return;
            }
            int parallelism = task.getInt("parallelism");
            taskToParallelism.put(taskName, parallelism);
            JSONArray subtasks = task.getJSONArray("subtasks");
            for (int j = 0; j < subtasks.length(); j++) {
                JSONObject subtask = subtasks.getJSONObject(j);
                String host = subtask.getString("host");
                String taskManagerId = subtask.getString("taskmanager-id");
                int subtaskIndex = subtask.getInt("subtask");
                subtaskToTM.put(taskName + "." + subtaskIndex, new Tuple2<>(host, taskManagerId));
            }
        }
    }

    private static Tuple2<String, String> getFirstRunningJobNameAndId(
            String jobManagerHost, int jobManagerPort) {
        try {
            JSONObject response =
                    NetworkUtils.httpGetJson(jobManagerHost, jobManagerPort, "/jobs/overview")
                            .toObject();
            JSONArray jobs = response.getJSONArray("jobs");
            for (int i = 0; i < jobs.length(); i++) {
                JSONObject job = jobs.getJSONObject(i);
                if (job.getString("state").equals("RUNNING")) {
                    return new Tuple2<>(job.getString("name"), job.getString("jid"));
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
