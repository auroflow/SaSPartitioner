package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import java.util.*;

public class WorkerStatistics {

    private final int numSlides;
    private final Map<Integer, Integer> total;
    private int totalCount;
    private final List<Map<Integer, Integer>> slides;
    private final int[] slideCounts;
    private int index = 0;

    public WorkerStatistics(int numSlides) {
        this.numSlides = numSlides;
        this.total = new HashMap<>();
        this.totalCount = 0;
        this.slides = new ArrayList<>(numSlides);
        for (int i = 0; i < numSlides; i++) {
            this.slides.add(new HashMap<>());
        }
        this.slideCounts = new int[numSlides];
    }

    public void add(int worker) {
        add(worker, 1);
    }

    public synchronized void add(int worker, int count) {
        Map<Integer, Integer> currentSlide = slides.get(index);
        currentSlide.put(worker, currentSlide.getOrDefault(worker, 0) + count);
        slideCounts[index] += count;
    }

    public synchronized int getTotalCount() {
        return totalCount + slideCounts[index];
    }

    public synchronized int getCount(int worker) {
        return total.getOrDefault(worker, 0) + slides.get(index).getOrDefault(worker, 0);
    }

    public synchronized int[] getWorkerCounts(int numWorkers) {
        int[] counts = new int[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            counts[i] = getCount(i);
        }
        return counts;
    }

    public synchronized void newSlide() {
        // add the current slide to total
        Map<Integer, Integer> currentSlide = slides.get(index);
        for (Map.Entry<Integer, Integer> entry : currentSlide.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue();
            total.put(key, total.getOrDefault(key, 0) + count);
        }
        totalCount += slideCounts[index];

        index = (index + 1) % numSlides;

        // subtract the oldest slide from total
        Map<Integer, Integer> oldestSlide = slides.get(index);
        for (Map.Entry<Integer, Integer> entry : oldestSlide.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue();
            total.put(key, total.getOrDefault(key, 0) - count);
        }
        totalCount -= slideCounts[index];

        // clear the oldest slide
        oldestSlide.clear();
        slideCounts[index] = 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int maxWorker =
                Math.max(
                        total.keySet().stream()
                                .max(Comparator.comparingInt(Integer::intValue))
                                .orElse(-1),
                        slides.get(index).keySet().stream()
                                .max(Comparator.comparingInt(Integer::intValue))
                                .orElse(-1));

        sb.append("[");
        for (int i = 0; i <= maxWorker; i++) {
            sb.append(getCount(i));
            if (i < maxWorker - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
