package cn.edu.zju.daily.metricflux.partitioner.container;

/**
 * stores a frequent key and its frequency used for the list storing the topKeys (to be forwarded to
 * the QTableReducer)
 */
public class Frequency {
    public int key;
    public int freq;

    public Frequency(int k, int f) {
        key = k;
        freq = f;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Frequency)) return false;
        Frequency other = (Frequency) o;
        return this.key == other.key;
    }

    @Override
    public final int hashCode() {
        return key;
    }
}
