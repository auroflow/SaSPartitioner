package cn.edu.zju.daily.metricflux.partitioner.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
public class HistogramStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(HistogramStatistics.class);

    private double min;
    private double max;
    private double mean;
    private double stddev;
    private double p50;
    private double p99;

    public enum Type {
        MIN,
        MAX,
        MEAN,
        STDDEV,
        P50,
        P99
    }

    public static HistogramStatistics parse(String value) {
        // {min-0.0,max-0.0,mean-0.0,stddev-0.0,p50-0.0,p99-0.0}
        String[] parts = value.substring(1, value.length() - 1).split(";");
        try {
            return new HistogramStatistics(
                    Double.parseDouble(parts[0].split("-")[1]),
                    Double.parseDouble(parts[1].split("-")[1]),
                    Double.parseDouble(parts[2].split("-")[1]),
                    Double.parseDouble(parts[3].split("-")[1]),
                    Double.parseDouble(parts[4].split("-")[1]),
                    Double.parseDouble(parts[5].split("-")[1]));
        } catch (Exception e) {
            LOG.error("Error when parsing {}", value, e);
            throw e;
        }
    }

    public double get(Type type) {
        switch (type) {
            case MIN:
                return min;
            case MAX:
                return max;
            case MEAN:
                return mean;
            case STDDEV:
                return stddev;
            case P50:
                return p50;
            case P99:
                return p99;
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    @Override
    public String toString() {
        return "{"
                + "min="
                + min
                + ", max="
                + max
                + ", mean="
                + mean
                + ", stddev="
                + stddev
                + ", p50="
                + p50
                + ", p99="
                + p99
                + '}';
    }

    public static void main(String[] args) {
        HistogramStatistics stat =
                HistogramStatistics.parse(
                        "{min-38,max-533,mean-170.25,stddev-242.2758414149734,p50-55.0,p99-533.0}");
        System.out.println(stat);
    }
}
