package cn.edu.zju.daily.metricflux.utils;

public class ThreadUtils {

    public static Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return (th, ex) -> {
            throw new RuntimeException("Uncaught exception in thread " + th.getName(), ex);
        };
    }
}
