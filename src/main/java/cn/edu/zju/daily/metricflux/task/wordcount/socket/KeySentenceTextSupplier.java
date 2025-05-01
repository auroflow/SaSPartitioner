package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import cn.edu.zju.daily.metricflux.core.socket.ZipfDistributionKeyGenerator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class KeySentenceTextSupplier implements Iterator<String> {

    private final long maxLines;
    private final Iterator<Integer> keyGenerator;
    private final Iterator<String> sentenceGenerator;
    private long count = 0;

    public KeySentenceTextSupplier(
            Iterator<Integer> keyGenerator, Iterator<String> sentenceGenerator, long maxLines) {
        this.keyGenerator = keyGenerator;
        this.sentenceGenerator = sentenceGenerator;
        this.maxLines = maxLines;
    }

    @Override
    public boolean hasNext() {
        return count < maxLines && keyGenerator.hasNext() && sentenceGenerator.hasNext();
    }

    @Override
    public String next() {
        if (count >= maxLines) {
            throw new NoSuchElementException();
        }
        if (!keyGenerator.hasNext()) {
            throw new IllegalStateException("keyGenerator has no more elements");
        }
        if (!sentenceGenerator.hasNext()) {
            throw new IllegalStateException("sentenceGenerator has no more elements");
        }
        count++;
        return keyGenerator.next() + "," + sentenceGenerator.next();
    }

    public static void main(String[] args) throws IOException {
        KeySentenceTextSupplier supplier =
                new KeySentenceTextSupplier(
                        new ZipfDistributionKeyGenerator(100000, 1.5),
                        new TextFileSentenceGenerator(
                                Path.of(
                                        "/home/user/code/saspartitioner/src/main/resources/pg135.txt"),
                                30),
                        10000000);

        String outputPath = "/home/user/Documents/zipf.csv";
        try (var writer = java.nio.file.Files.newBufferedWriter(Path.of(outputPath))) {
            while (supplier.hasNext()) {
                writer.write(supplier.next());
                writer.newLine();
            }
        }
    }
}
