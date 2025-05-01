package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import static smile.math.MathEx.sum;

import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

public class KeyWorkerStatistics<K> {

    private final int numSlides;
    private final int numWorkers;

    private final Map<K, int[]> aggregated;
    private final List<Map<K, int[]>> slides;
    private final int[] slideCounts;
    private int aggregatedCount;
    private int index = 0;
    private boolean fullWindow = false;

    public KeyWorkerStatistics(int numSlides, int numWorkers) {
        this.numSlides = numSlides;
        this.aggregated = new HashMap<>();
        this.aggregatedCount = 0;
        this.slides = new ArrayList<>(numSlides);
        for (int i = 0; i < numSlides; i++) {
            this.slides.add(new HashMap<>());
        }
        this.slideCounts = new int[numSlides];
        this.numWorkers = numWorkers;
    }

    public void add(K key, int worker) {
        add(key, worker, 1);
    }

    public synchronized void add(K key, int worker, int count) {
        Map<K, int[]> currentSlide = slides.get(index);
        currentSlide.computeIfAbsent(key, k -> new int[numWorkers])[worker] += count;
        slideCounts[index] += count;
    }

    public synchronized void newSlide() {
        // add the current slide to total
        Map<K, int[]> currentSlide = slides.get(index);
        for (Map.Entry<K, int[]> entry : currentSlide.entrySet()) {
            K key = entry.getKey();
            int[] count = entry.getValue();
            int[] aggregatedArray = aggregated.computeIfAbsent(key, k -> new int[numWorkers]);
            for (int i = 0; i < numWorkers; i++) {
                aggregatedArray[i] += count[i];
            }
        }
        aggregatedCount += slideCounts[index];

        index = (index + 1) % numSlides;
        if (index == 0) {
            fullWindow = true;
        }

        // subtract the oldest slide from total
        Map<K, int[]> oldestSlide = slides.get(index);
        for (Map.Entry<K, int[]> entry : oldestSlide.entrySet()) {
            K key = entry.getKey();
            int[] count = entry.getValue();
            int[] aggregatedArray = aggregated.get(key);
            boolean clear = true;
            for (int i = 0; i < numWorkers; i++) {
                aggregatedArray[i] -= count[i];
                if (aggregatedArray[i] > 0) {
                    clear = false;
                }
            }
            if (clear) {
                aggregated.remove(key);
            }
        }
        aggregatedCount -= slideCounts[index];

        // clear the oldest slide
        oldestSlide.clear();
        slideCounts[index] = 0;
    }

    public synchronized int getSlideAverage(int pastSlides) {
        if (pastSlides < 0 || pastSlides > this.numSlides) {
            throw new IllegalArgumentException("Invalid number of slides: " + pastSlides);
        }
        int total = 0;
        if (!fullWindow) {
            pastSlides = Math.min(pastSlides, this.index);
        }
        for (int i = 0; i < pastSlides; i++) {
            total += slideCounts[(numSlides + index - i) % numSlides];
        }
        return total / pastSlides;
    }

    public synchronized int getTotalCount(boolean lastSlide) {
        if (lastSlide) {
            return slideCounts[index];
        } else {
            return aggregatedCount + slideCounts[index];
        }
    }

    public int getTotalCount() {
        return getTotalCount(false);
    }

    public synchronized Set<K> getKeys(boolean lastSlide) {
        Set<K> keys = new HashSet<>();
        if (!lastSlide) {
            keys.addAll(aggregated.keySet());
        }
        keys.addAll(slides.get(index).keySet());
        return keys;
    }

    /**
     * Get hot keys with count.
     *
     * @return hot keys with count
     */
    public Map<K, Integer> getHotKeyCounts(double hotKeyFactor, boolean lastSlide) {
        Set<K> allKeys = getKeys(lastSlide);
        int baseThreshold = getTotalCount() / numWorkers;

        List<Pair<K, Integer>> occurrences = new ArrayList<>();
        int aboveBaseThreshold = 0;
        for (K key : allKeys) {
            int count = getCount(key, lastSlide);
            if (count >= baseThreshold) {
                aboveBaseThreshold++;
            }
            occurrences.add(Pair.of(key, count));
        }
        occurrences.sort(Comparator.<Pair<K, Integer>>comparingInt(Pair::getValue).reversed());
        int numHotKeys = Math.min(allKeys.size(), (int) (hotKeyFactor * aboveBaseThreshold));
        Map<K, Integer> hotKeys = new HashMap<>();
        for (int i = 0; i < numHotKeys; i++) {
            hotKeys.put(occurrences.get(i).getKey(), occurrences.get(i).getValue());
        }
        return hotKeys;
    }

    public synchronized Map<K, Integer> getHotKeyCounts(int numHotKeys, boolean lastSlide) {
        Set<K> allKeys = getKeys(lastSlide);

        List<Pair<K, Integer>> occurrences = new ArrayList<>();
        for (K key : allKeys) {
            int count = getCount(key, lastSlide);
            occurrences.add(Pair.of(key, count));
        }
        occurrences.sort(Comparator.<Pair<K, Integer>>comparingInt(Pair::getValue).reversed());
        Map<K, Integer> hotKeys = new HashMap<>();
        numHotKeys = Math.min(numHotKeys, occurrences.size());
        for (int i = 0; i < numHotKeys; i++) {
            hotKeys.put(occurrences.get(i).getKey(), occurrences.get(i).getValue());
        }
        return hotKeys;
    }

    public synchronized int getCount(K key, boolean lastSlide) {
        Map<K, int[]> currentSlide = slides.get(index);
        int total = 0;
        if (!lastSlide && aggregated.containsKey(key)) {
            total += Arrays.stream(aggregated.get(key)).sum();
        }
        if (currentSlide.containsKey(key)) {
            total += Arrays.stream(currentSlide.get(key)).sum();
        }
        return total;
    }

    public synchronized Map<K, int[]> getWorkerCounts(boolean lastSlide) {
        Set<K> allKeys = getKeys(lastSlide);
        Map<K, int[]> workerCounts = new HashMap<>();
        for (K key : allKeys) {
            workerCounts.put(key, getWorkerCount(key, lastSlide));
        }
        return workerCounts;
    }

    public synchronized Map<K, double[]> getWorkerRatios(boolean lastSlide) {
        Map<K, int[]> workerCounts = getWorkerCounts(lastSlide);
        Map<K, double[]> workerRatios = new HashMap<>();
        for (Map.Entry<K, int[]> entry : workerCounts.entrySet()) {
            K key = entry.getKey();
            int[] counts = entry.getValue();
            long sum = sum(counts);
            double[] ratios = new double[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
                ratios[i] = (double) counts[i] / sum;
            }
            workerRatios.put(key, ratios);
        }
        return workerRatios;
    }

    public synchronized int[] getWorkerCount(K key, boolean lastSlide) {
        int[] total = new int[numWorkers];
        Map<K, int[]> currentSlide = slides.get(index);
        if (currentSlide.containsKey(key)) {
            int[] count = currentSlide.get(key);
            System.arraycopy(count, 0, total, 0, numWorkers);
        }
        if (!lastSlide && aggregated.containsKey(key)) {
            int[] count = aggregated.get(key);
            for (int i = 0; i < numWorkers; i++) {
                total[i] += count[i];
            }
        }
        return total;
    }
}
