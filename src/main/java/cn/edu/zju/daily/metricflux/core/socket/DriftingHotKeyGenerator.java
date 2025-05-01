package cn.edu.zju.daily.metricflux.core.socket;

import java.util.Iterator;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DriftingHotKeyGenerator implements Iterator<Integer> {

    private final Iterator<Integer> base;
    private final int driftingKey;
    private final double minProb;
    private final double maxProb;
    private final double midProb;
    private final long shiftInterval; // seconds
    private final Random random;

    private long lastUpdateTS = 0;
    private double prob;

    public DriftingHotKeyGenerator(
            Iterator<Integer> base,
            int driftingKey,
            double minProb,
            double maxProb,
            long shiftInterval) {
        this.base = base;
        this.driftingKey = driftingKey;
        this.minProb = minProb;
        this.maxProb = maxProb;
        this.midProb = (minProb + maxProb) / 2;
        this.shiftInterval = shiftInterval;
        this.prob = minProb;
        this.random = new Random();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Integer next() {
        long now = System.currentTimeMillis();
        if (now - lastUpdateTS > shiftInterval * 1000) {
            lastUpdateTS = now;
            if (prob < midProb) {
                prob = midProb + random.nextDouble() * (maxProb - midProb);
            } else {
                prob = minProb + random.nextDouble() * (midProb - minProb);
            }
            LOG.info("New probability for key {}: {}", driftingKey, prob);
        }
        if (random.nextDouble() < prob) {
            return driftingKey;
        } else {
            return base.next();
        }
    }
}
