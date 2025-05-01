package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import static java.util.stream.Collectors.joining;

import java.util.Iterator;
import java.util.stream.IntStream;

/** Generates a string consisting of 20 random numbers separated by spaces. */
public class RandomNumberSentenceGenerator implements Iterator<String> {

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public String next() {
        return IntStream.range(0, 20)
                .map(i -> (int) (Math.random() * 10000))
                .mapToObj(Integer::toString)
                .collect(joining(" "));
    }
}
