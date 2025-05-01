package cn.edu.zju.daily.metricflux.task.wordcount.data;

import cn.edu.zju.daily.metricflux.core.data.FinalResult;
import cn.edu.zju.daily.metricflux.core.data.PartialResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class WordCountState implements PartialResult<Integer>, FinalResult<Integer> {

    private long ts;
    private Integer key;
    private Map<String, Integer> counts;
    private boolean hot = false;

    public WordCountState(int key) {
        this.key = key;
        counts = new HashMap<>();
    }

    public void count(String word) {
        count(word, 1);
    }

    public void count(String word, int times) {
        counts.put(word, counts.getOrDefault(word, 0) + times);
    }

    public void merge(WordCountState other) {
        if (!Objects.equals(this.key, other.key)) {
            throw new IllegalArgumentException("Cannot merge states with different keys");
        }
        for (Map.Entry<String, Integer> entry : other.counts.entrySet()) {
            this.count(entry.getKey(), entry.getValue());
        }
        this.hot = this.hot || other.hot;
    }

    @Override
    public FinalResult<Integer> toFinal() {
        return this;
    }
}
