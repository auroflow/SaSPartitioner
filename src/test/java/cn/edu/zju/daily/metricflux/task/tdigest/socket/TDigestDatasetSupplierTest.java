package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import org.junit.jupiter.api.Test;

class TDigestDatasetSupplierTest {

    private static final String DATASET_FILE = "/home/user/code/data/london_smart_meters.csv";
    private static final String FREQ_FILE = "/home/user/code/data/london_smart_meters-freq.csv";

    @Test
    void testReadData() throws IOException {
        TDigestDatasetSupplier supplier = new TDigestDatasetSupplier(DATASET_FILE);
        Iterator<Integer> keySupplier = supplier.getKeySupplier();
        Map<Integer, Integer> counts = new HashMap<>();
        int numLines = supplier.getNumLines();
        for (int i = 0; i < numLines; i++) {
            int key = keySupplier.next();
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }

        // Read FREQ_FILE using CSV library
        Map<Integer, Integer> groundtruth = new HashMap<>();
        try (Scanner scanner = new Scanner(new File(FREQ_FILE))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] parts = line.split(",");
                int key = Integer.parseInt(parts[0]);
                int count = Integer.parseInt(parts[2]);
                groundtruth.put(key, count);
            }
        }

        // Compare counts with groundtruth
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue();
            assertTrue(groundtruth.containsKey(key));
            assertEquals(count, groundtruth.get(key));
        }
    }
}
