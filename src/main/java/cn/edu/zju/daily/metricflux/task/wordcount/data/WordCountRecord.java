package cn.edu.zju.daily.metricflux.task.wordcount.data;

import cn.edu.zju.daily.metricflux.core.data.AbstractRecord;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class WordCountRecord extends AbstractRecord<Integer> {

    private String str;

    public WordCountRecord() {}

    public WordCountRecord(int key, long ts, String str) {
        super(key, ts);
        this.str = str;
    }

    @Override
    public String toString() {
        return str;
    }
}
