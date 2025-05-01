package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import static java.util.stream.Collectors.toList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A supplier that reads a dataset from a file.
 *
 * <p>The dataset is expected to be in the format: key,value1,value2,...
 *
 * <p>The data index only progresses when calling getKeySupplier().next().
 */
@Slf4j
public class TDigestDatasetSupplier {

    private final List<Integer> keys;
    private final List<List<Double>> values;
    private final long numRecords;

    @Getter private final int maxValueSize;

    private int currentIndex = -1;
    private long currentCount = 0;

    public TDigestDatasetSupplier(String dataPath) throws IOException {
        this(dataPath, Integer.MAX_VALUE);
    }

    public TDigestDatasetSupplier(String datasetPath, long numRecords) throws IOException {

        keys = new ArrayList<>();
        values = new ArrayList<>();

        // Load the dataset
        // Each line is key + "," + value1 + "," + value2 + "," + ...
        try (Stream<String> lines = Files.lines(Path.of(datasetPath))) {
            lines.limit(numRecords)
                    .forEach(
                            line -> {
                                String[] parts = line.split(",");
                                keys.add(Integer.parseInt(parts[0]));
                                List<Double> valueList = new ArrayList<>();
                                for (int i = 1; i < parts.length; i++) {
                                    valueList.add(Double.parseDouble(parts[i]));
                                }
                                values.add(valueList);
                            });
        }

        maxValueSize =
                values.stream()
                        .mapToInt(List::size)
                        .max()
                        .orElseThrow(
                                () -> new IllegalArgumentException("No values found in dataset"));
        this.numRecords = numRecords;

        LOG.info(
                "TDigestDatasetSupplier: loaded {} records, hot keys: {}",
                getNumLines(),
                getHotKeys(20));
    }

    public int getNumLines() {
        return keys.size();
    }

    public List<Map.Entry<Integer, Integer>> getHotKeys(int numHotKeys) {
        // count the number of occurrences of each key
        Map<Integer, Integer> keyCounts =
                keys.stream()
                        .collect(
                                HashMap::new,
                                (map, key) -> map.merge(key, 1, Integer::sum),
                                HashMap::putAll);
        // sort the keys by their counts
        return keyCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(numHotKeys)
                .collect(toList());
    }

    @Getter
    private final Iterator<Integer> keySupplier =
            new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return currentCount < numRecords;
                }

                @Override
                public Integer next() {
                    currentIndex = (currentIndex + 1) % keys.size();
                    currentCount++;
                    return keys.get(currentIndex);
                }
            };

    @Getter
    private final TDigestValueSupplier valueSupplier =
            new TDigestValueSupplier() {

                private final byte[] bytes = new byte[Double.BYTES];

                @Override
                protected void trySupply(DataOutputStream out) throws IOException {
                    List<Double> valueList = values.get(currentIndex);
                    out.writeInt(valueList.size());
                    for (double value : valueList) {
                        out.writeDouble(value);
                    }
                }
            };
}
