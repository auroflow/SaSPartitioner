package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import static smile.math.MathEx.sum;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyStatistics<K> {

    private final int numSlides;
    private final int numWorkers;
    private final Map<K, Integer> aggregated;
    private final List<Map<K, Integer>> slides;
    private int aggregatedCount;
    private final int[] slideCounts;
    private int index = 0;

    public KeyStatistics(int numSlides, int numWorkers) {
        this.numSlides = numSlides;
        this.aggregated = new ConcurrentHashMap<>();
        this.aggregatedCount = 0;
        this.slides = new ArrayList<>(numSlides);
        for (int i = 0; i < numSlides; i++) {
            this.slides.add(new ConcurrentHashMap<>());
        }
        this.slideCounts = new int[numSlides];
        this.numWorkers = numWorkers;
    }

    public void add(K key) {
        add(key, 1);
    }

    public void add(K key, int count) {
        Map<K, Integer> currentSlide = slides.get(index);
        currentSlide.put(key, currentSlide.getOrDefault(key, 0) + count);
        slideCounts[index] += count;
    }

    public int getTotalCount() {
        return aggregatedCount + slideCounts[index];
    }

    public int getCount(K key) {
        Map<K, Integer> currentSlide = slides.get(index);
        return aggregated.getOrDefault(key, 0) + currentSlide.getOrDefault(key, 0);
    }

    public int getCount(K key, boolean currentSlideOnly) {
        Map<K, Integer> current = slides.get(index);
        if (currentSlideOnly) {
            return current.getOrDefault(key, 0);
        }
        return aggregated.getOrDefault(key, 0) + current.getOrDefault(key, 0);
    }

    public Map<K, Integer> getCounts() {
        Map<K, Integer> currentSlide = slides.get(index);
        Map<K, Integer> counts = new HashMap<>(aggregated);
        for (Map.Entry<K, Integer> entry : currentSlide.entrySet()) {
            K key = entry.getKey();
            int count = entry.getValue();
            counts.put(key, counts.getOrDefault(key, 0) + count);
        }
        return counts;
    }

    public Set<K> getKeys() {
        Set<K> keys = new HashSet<>();
        keys.addAll(aggregated.keySet());
        keys.addAll(slides.get(index).keySet());
        return keys;
    }

    public void newSlide() {
        // add the current slide to total
        Map<K, Integer> currentSlide = slides.get(index);
        for (Map.Entry<K, Integer> entry : currentSlide.entrySet()) {
            K key = entry.getKey();
            int count = entry.getValue();
            aggregated.put(key, aggregated.getOrDefault(key, 0) + count);
        }
        aggregatedCount += slideCounts[index];

        index = (index + 1) % numSlides;

        // subtract the oldest slide from total
        Map<K, Integer> oldestSlide = slides.get(index);
        for (Map.Entry<K, Integer> entry : oldestSlide.entrySet()) {
            K key = entry.getKey();
            int count = entry.getValue();
            int newValue = aggregated.getOrDefault(key, 0) - count;
            if (newValue > 0) {
                aggregated.put(key, newValue);
            } else {
                aggregated.remove(key);
            }
        }
        aggregatedCount -= slideCounts[index];

        // clear the oldest slide
        oldestSlide.clear();
        slideCounts[index] = 0;
    }

    /**
     * Get hot keys with count.
     *
     * @return hot keys with count
     */
    public Map<K, Integer> getHotKeysWithCount(double hotKeyFactor) {
        Set<K> allKeys = getKeys();
        int baseThreshold = getTotalCount() / numWorkers;

        List<Tuple2<K, Integer>> occurrences = new ArrayList<>();
        int aboveBaseThreshold = 0;
        for (K key : allKeys) {
            int count = getCount(key);
            if (count >= baseThreshold) {
                aboveBaseThreshold++;
            }
            occurrences.add(Tuple2.of(key, count));
        }
        occurrences.sort(Comparator.<Tuple2<K, Integer>>comparingInt(t -> t.f1).reversed());
        int numHotKeys = Math.min(allKeys.size(), (int) (hotKeyFactor * aboveBaseThreshold));
        Map<K, Integer> hotKeys = new HashMap<>();
        for (int i = 0; i < numHotKeys; i++) {
            hotKeys.put(occurrences.get(i).f0, occurrences.get(i).f1);
        }
        return hotKeys;
    }

    /**
     * Get hot keys with count.
     *
     * @param numHotKeys number of hot keys
     * @return
     */
    public Map<K, Integer> getHotKeysWithCount(int numHotKeys, boolean currentSlideOnly) {
        Map<K, Integer> currentSlide = slides.get(index);
        Set<K> allKeys = currentSlideOnly ? currentSlide.keySet() : getKeys();

        List<Tuple2<K, Integer>> occurrences = new ArrayList<>();
        for (K key : allKeys) {
            int count = getCount(key, currentSlideOnly);
            occurrences.add(Tuple2.of(key, count));
        }
        occurrences.sort(Comparator.<Tuple2<K, Integer>>comparingInt(t -> t.f1).reversed());
        numHotKeys = Math.min(allKeys.size(), numHotKeys);
        Map<K, Integer> hotKeys = new HashMap<>();
        for (int i = 0; i < numHotKeys; i++) {
            hotKeys.put(occurrences.get(i).f0, occurrences.get(i).f1);
        }
        return hotKeys;
    }

    public Pair<List<K>, double[]> getHotKeysWithDistribution(
            int numHotKeys, boolean currentSlideOnly) {
        Map<K, Integer> hotKeysWithCount = getHotKeysWithCount(numHotKeys, currentSlideOnly);

        List<K> hotKeys = new ArrayList<>(hotKeysWithCount.keySet());
        hotKeys.sort(Comparator.comparingInt(hotKeysWithCount::get).reversed());

        double[] dist = new double[numHotKeys];
        for (int i = 0; i < numHotKeys; i++) {
            if (i < hotKeys.size()) {
                dist[i] = hotKeysWithCount.get(hotKeys.get(i));
            } else {
                dist[i] = 0;
            }
        }

        double sum = sum(dist);
        for (int i = 0; i < dist.length; i++) {
            dist[i] /= sum;
        }
        return Pair.of(List.copyOf(hotKeys), dist);
    }
}
