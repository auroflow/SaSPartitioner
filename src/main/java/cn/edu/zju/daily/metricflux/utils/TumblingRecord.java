package cn.edu.zju.daily.metricflux.utils;

public class TumblingRecord {
    private int index = 0;
    private double sum = 0d;
    private double value = 0;
    private int size;

    public TumblingRecord(int size) {
        this.size = size;
    }

    public void append(double value) {
        sum += value;
        if (index == size - 1) {
            value = sum / size;
            sum = 0;
            index = 0;
        } else {
            index++;
        }
    }

    public double value() {
        return value;
    }
}
