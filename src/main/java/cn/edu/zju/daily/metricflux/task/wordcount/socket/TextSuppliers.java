package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import cn.edu.zju.daily.metricflux.core.socket.DriftingHotKeyGenerator;
import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import cn.edu.zju.daily.metricflux.utils.Parameters;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class TextSuppliers {

    public static Iterator<String> getSupplier(Parameters params) throws IOException {
        String dataset = params.getDataset();
        if ("t4sa".equals(dataset)) {
            return new CSVTextSupplier(Path.of(params.getDatasetPath()), params.getNumRecords());
        } else if ("zipf".equals(dataset)) {
            return new KeySentenceTextSupplier(
                    new ZipfDistributionKeyGenerator(params.getNumKeys(), params.getZipf()),
                    new TextFileSentenceGenerator("pg135.txt", params.getMaxWordsPerLine()),
                    params.getNumRecords());
        } else if ("zipf-sequence".equals(dataset)) {
            return new KeySentenceTextSupplier(
                    new ZipfDistributionKeyGenerator(
                            params.getNumKeysSequence(),
                            params.getZipfSequence(),
                            params.getShiftInterval(),
                            params.getShiftAlignment(),
                            params.isShuffleKey()),
                    new TextFileSentenceGenerator("pg135.txt", params.getMaxWordsPerLine()),
                    params.getNumRecords());
        } else if ("zipf-drifting-key".equals(dataset)) {
            return new KeySentenceTextSupplier(
                    new DriftingHotKeyGenerator(
                            new ZipfDistributionKeyGenerator(params.getNumKeys(), params.getZipf()),
                            params.getNumKeys(), // the last key is the drifting key
                            0.001,
                            0.03,
                            params.getShiftInterval()),
                    new TextFileSentenceGenerator("pg135.txt", params.getMaxWordsPerLine()),
                    params.getNumRecords());
        } else {
            throw new IllegalArgumentException("Unknown dataset: " + dataset);
        }
    }
}
