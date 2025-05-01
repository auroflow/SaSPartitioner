package cn.edu.zju.daily.metricflux.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class RoutingTableUtilsTest {

    @Test
    void testGetZipfDistribution() throws IOException {
        double[] p = RoutingTableUtils.getZipfProbabilities(1.5, 100000);
        // Save
        try (var writer = java.nio.file.Files.newBufferedWriter(Path.of("zipf.csv"))) {
            for (int i = 0; i < p.length; i++) {
                writer.write(i + "," + i + "," + p[i]);
                writer.newLine();
            }
        }
    }
}
