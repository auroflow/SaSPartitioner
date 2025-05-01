package cn.edu.zju.daily.metricflux.utils.rate;

public interface RateLimiter {

    /**
     * Get the delay for the next emit in nanoseconds.
     *
     * @param count
     * @return
     */
    long getDelayNanos(long count);
}
