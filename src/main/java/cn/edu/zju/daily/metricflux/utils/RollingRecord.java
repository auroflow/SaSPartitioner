package cn.edu.zju.daily.metricflux.utils;

public class RollingRecord {
    private final double[] values;
    private int index = 0;
    private boolean full = false;
    private double sum = 0d;

    public RollingRecord(int size) {
        this.values = new double[size];
    }

    public void append(double value) {
        sum += value - values[index];
        values[index] = value;
        index = (index + 1) % values.length;
        if (index == 0) {
            full = true;
        }
    }

    public double value() {
        int count = full ? values.length : index;
        return count == 0 ? 0 : sum / count;
    }
}
