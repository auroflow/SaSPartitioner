package cn.edu.zju.daily.metricflux.partitioner.learning.exceptions;

public class EpisodeAlreadyStartedException extends RuntimeException {

    public EpisodeAlreadyStartedException(String message) {
        super(message);
    }
}
