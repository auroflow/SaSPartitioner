/*Copyright (c) 2022 Data Intensive Applications and Systems Laboratory (DIAS)
                   Ecole Polytechnique Federale de Lausanne
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

package cn.edu.zju.daily.metricflux.partitioner.container;

import cn.edu.zju.daily.metricflux.core.data.Record;
import java.io.Serializable;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
/** Class responsible for maintaining frequency statistics */
public class HotStatistics implements Serializable {
    private CountMinSketch countMinSketch;
    private Set<Integer> hotKeys;
    private int total;
    private Map<Integer, Integer> keysStatistics;
    private double threshold;
    private boolean usingSketch;

    private long hotInterval;
    private long nextUpdateHot;
    private int numWorkers;

    public HotStatistics(int numWorkers, int estimatedNumKeys, long hotInterval) {
        countMinSketch = new CountMinSketch(2.0 / hotInterval, 0.9, 33);
        hotKeys = new HashSet<>(numWorkers);
        total = 0;
        threshold = Double.MAX_VALUE;
        keysStatistics = new HashMap<>(estimatedNumKeys);
        usingSketch = false;

        this.hotInterval = hotInterval;
        nextUpdateHot = 0;
        this.numWorkers = numWorkers;
    }

    /**
     * @param tuple the newly arrived tuple
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last
     *     tuple, expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    public long isHotDAG(Record<Integer> tuple) {
        if (nextUpdateHot == 0) {
            nextUpdateHot = tuple.getTs() + hotInterval;
        }

        boolean isHot = hotKeys.contains(tuple.getKey());
        total++;
        long result = 1; // 1 means hot, 0 not hot

        if (!isHot) {
            int prev = keysStatistics.getOrDefault(tuple.getKey(), 0);
            keysStatistics.put(tuple.getKey(), prev + 1);
            if (prev + 1 > threshold) {
                hotKeys.add(tuple.getKey());
                isHot = true;
                result = nextUpdateHot + hotInterval;
            } else {
                result = 0;
            }
        }
        if (tuple.getTs() >= nextUpdateHot) {
            threshold = (double) total / numWorkers;
            total = 0;
            hotKeys.clear();
            nextUpdateHot += hotInterval;
            keysStatistics.clear();
        }

        return result;
    }

    /**
     * @param tuple the newly arrived tuple
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last
     *     tuple, expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    private long isHotExact(Record<Integer> tuple, List<Frequency> topKeys) {
        boolean isHot = hotKeys.contains(tuple.getKey());
        total++;
        long result = 1; // 1 means hot, 0 not hot
        int freq = Integer.MAX_VALUE;
        if (!isHot) {
            freq = keysStatistics.getOrDefault(tuple.getKey(), 0) + 1;
            keysStatistics.put(tuple.getKey(), freq);
            if (freq > threshold) {
                hotKeys.add(tuple.getKey());
                isHot = true;
                result = nextUpdateHot + hotInterval;
            } else {
                result = 0;
            }
        }

        if (topKeys != null) {
            updateTopKeys(tuple.getKey(), freq, topKeys);
        }
        return result;
    }

    /**
     * @param tuple the newly arrived tuple
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last
     *     tuple, expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    private long isHotSketch(Record<Integer> tuple, List<Frequency> topKeys) {
        boolean isHot = hotKeys.contains(tuple.getKey());
        int freq = Integer.MAX_VALUE;
        total++;
        long result = 1; // 1 means hot, 0 not hot
        if (!isHot) {
            freq = countMinSketch.add_and_estimate(tuple.getKey(), 1);
            if (freq > threshold) {
                hotKeys.add(tuple.getKey());
                isHot = true;
                result = nextUpdateHot + hotInterval;
            } else {
                result = 0;
            }
        }

        if (topKeys != null) {
            updateTopKeys(tuple.getKey(), freq, topKeys);
        }
        return result;
    }

    /**
     * @param tuple the newly arrived tuple
     * @param numOfDistinctKeys number of distinct keys (used to decide whether to use countMin
     *     sketch or exact stats
     * @param topKeys a list maintaining the top n (n=numOfWorkers) most frequent keys (used in the
     *     multi-agent setup to forward them to the QTableReducer). In the single-partitioner setup
     *     topKeys = null
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last
     *     tuple, expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    public long isHot(Record<Integer> tuple, int numOfDistinctKeys, List<Frequency> topKeys) {
        if (nextUpdateHot == 0) {
            // align with event time window
            nextUpdateHot = tuple.getTs() + hotInterval;
            nextUpdateHot += (hotInterval - (nextUpdateHot % hotInterval)) % hotInterval;
            LOG.info("Initializing nextUpdateHot to {}", nextUpdateHot);
        }
        if (tuple.getTs() >= nextUpdateHot) {
            if (usingSketch) {
                countMinSketch.clear();
            } else {
                keysStatistics.clear();
            }
            usingSketch = (numOfDistinctKeys >= 10000);
            hotKeys.clear();
            nextUpdateHot += hotInterval;
            if (topKeys != null) {
                topKeys.clear();
            } else {
                threshold = (double) total / numWorkers;
            }
            total = 0;
        }
        return usingSketch ? isHotSketch(tuple, topKeys) : isHotExact(tuple, topKeys);
    }

    public void updateTopKeys(int key, int freq, List<Frequency> topKeys) {
        int pos = topKeys.indexOf(key);
        if (topKeys.isEmpty()) {
            Frequency updatedKey = new Frequency(key, freq);
            topKeys.add(updatedKey);
            return;
        }
        if (pos > -1) { // key in topKeys already
            Frequency updatedKey = topKeys.get(pos);
            topKeys.get(pos).freq = freq;
            int i = pos - 1;
            while (i >= 0 && freq > topKeys.get(i).freq) {
                i--;
            }
            if (i + 1 != pos) {
                topKeys.remove(pos);
                topKeys.add(i + 1, updatedKey);
            }
        } else if (topKeys.size() < numWorkers) {
            Frequency updatedKey = new Frequency(key, freq);
            int i = topKeys.size() - 1;
            while (i >= 0 && freq > topKeys.get(i).freq) {
                i--;
            }
            topKeys.add(i + 1, updatedKey);
        } else {
            int i = topKeys.size() - 1;
            while (i >= 0 && freq > topKeys.get(i).freq) {
                i--;
            }
            if (i != topKeys.size() - 1) {
                Frequency updatedKey = new Frequency(key, freq);
                topKeys.add(i + 1, updatedKey);
                topKeys.remove(topKeys.size() - 1);
            }
        }
    }

    public long getExpirationTs() {
        return nextUpdateHot + hotInterval;
    }

    public int getTotal() {
        return total;
    }

    public void setFrequencyThreshold(int t) {
        threshold = (double) t / numWorkers;
    }

    public void setHotInterval(int h) {
        hotInterval = h;
    }
}
