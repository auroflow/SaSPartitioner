package cn.edu.zju.daily.metricflux.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class DataUtils {

    /**
     * Read distribution file.
     *
     * @param path file path
     * @return a list of integers, each representing the count of a key
     */
    public static List<Integer> readDistribution(String path) {
        List<Integer> dist = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            // keyId,key,count
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int count = Integer.parseInt(parts[parts.length - 1]);
                dist.add(count);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading distribution file: " + path, e);
        }

        // Check if the frequencies are sorted in descending order
        for (int i = 0; i < dist.size() - 1; i++) {
            if (dist.get(i) < dist.get(i + 1)) {
                throw new IllegalArgumentException(
                        "Distribution file " + path + " is not sorted in descending order");
            }
        }

        return dist;
    }
}
