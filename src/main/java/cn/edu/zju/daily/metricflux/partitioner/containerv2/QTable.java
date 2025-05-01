package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QTable<K> {

    private final Map<K, double[]> table; // key -> values
    private final int numWorkers;
    private final double a;
    private final double initialValue;

    /**
     * Constructor for QTable class.
     *
     * @param numWorkers number of workers
     * @param a learning rate (weight of latest reward)
     */
    public QTable(int numWorkers, double a, double initialValue) {
        this.numWorkers = numWorkers;
        table = new HashMap<>();
        this.a = a;
        this.initialValue = initialValue;
    }

    public boolean containsKey(K key) {
        return table.containsKey(key);
    }

    public Set<K> keySet() {
        return table.keySet();
    }

    public void setTable(Map<K, double[]> table) {
        this.table.clear();
        this.table.putAll(table);
    }

    public void appendTable(Map<K, double[]> table) {
        this.table.putAll(table);
    }

    public double[] get(K key) {
        double[] values = table.get(key);
        if (values == null) {
            throw new IllegalArgumentException("Key " + key + " not found in QTable");
        }
        return values;
    }

    public void put(K key, double[] values) {
        if (values.length != numWorkers) {
            throw new IllegalArgumentException(
                    "Values length " + values.length + " not equal to numWorkers " + numWorkers);
        }
        table.put(key, values);
    }

    public double[] getOrDefault(K key) {
        double[] values = table.get(key);
        if (values == null) {
            double[] initialValues = new double[numWorkers];
            Arrays.fill(initialValues, initialValue);
            return initialValues;
        }
        return values;
    }

    /**
     * Remove a key from the qtable, returning the values.
     *
     * @param key key to remove
     * @return values
     */
    public double[] remove(K key) {
        if (!table.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " not found in QTable");
        }
        return table.remove(key);
    }

    public void update(K key, int worker, double reward) {
        if (!table.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " not found in QTable");
        }
        double[] values = table.get(key);
        values[worker] = (1 - a) * values[worker] + a * reward;
    }

    public void addKeyIfNotPresent(K key) {
        if (!table.containsKey(key)) {
            double[] values = new double[numWorkers];
            Arrays.fill(values, initialValue);
            table.put(key, values);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // header
        sb.append("|   key   |");
        for (int i = 0; i < numWorkers; i++) {
            sb.append(String.format("   %2d|", i));
        }
        for (K key : table.keySet()) {
            sb.append("\n|");
            String str = key.toString();
            sb.append(String.format("%10s|", key));
            double[] values = table.get(key);
            for (double value : values) {
                sb.append(String.format("%5.3f|", value));
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        QTable<Integer> qtable = new QTable<>(5, 0.1, 0.0);
        for (int i = 0; i < 5; i++) {
            qtable.addKeyIfNotPresent(i);
            qtable.update(i, i, i + 1);
        }
        System.out.println(qtable);
    }
}
