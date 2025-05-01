package cn.edu.zju.daily.metricflux.utils.rate;

import java.util.List;

public class FixedRateLimiter implements RateLimiter {

    private final StagedRateLimiter proxy;

    public FixedRateLimiter(long delay) {
        proxy = new StagedRateLimiter(List.of(0L), List.of(delay));
    }

    @Override
    public long getDelayNanos(long count) {
        return proxy.getDelayNanos(count);
    }
}
