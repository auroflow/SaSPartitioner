package cn.edu.zju.daily.metricflux.utils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    private static final JSONString NULL = new JSONString("");

    @Slf4j
    public static class JSONString {
        private final String json;

        public JSONString(String json) {
            this.json = json;
        }

        public JSONObject toObject() {
            try {
                return new JSONObject(json);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse JSON string: " + json, e);
            }
        }

        public JSONArray toArray() {
            return new JSONArray(json);
        }
    }

    public static JSONString httpGetJson(String host, int port, String url) {
        return httpGetJson(host, port, url, false);
    }

    public static JSONString httpGetJson(String host, int port, String url, boolean log) {

        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(new URI("http://" + host + ":" + port + url))
                            .timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json")
                            .GET()
                            .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            int status = response.statusCode();
            if (status != 200) {
                return NULL;
            }
            String body = response.body();
            if (log) {
                LOG.info("GET {}:{}{}: {}", host, port, url, body);
            }
            return new JSONString(body);
        } catch (Exception ignored) {
            return NULL;
        }
    }

    public static int httpPatch(String host, int port, String url) {

        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(new URI("http://" + host + ":" + port + url))
                            .timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json")
                            .method("PATCH", HttpRequest.BodyPublishers.noBody())
                            .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            return response.statusCode();
        } catch (Exception ignored) {
            return -1;
        }
    }
}
