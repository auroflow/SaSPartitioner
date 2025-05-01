package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class TextFileSentenceGenerator implements Iterator<String> {

    private final List<String> lines;
    private int count;
    private final int linesPerTime;

    public TextFileSentenceGenerator(Path path, int maxWordsPerLine) throws IOException {
        this(path, maxWordsPerLine, 1);
    }

    public TextFileSentenceGenerator(Path path, int maxWordsPerLine, int linesPerTime)
            throws IOException {
        this(path.toUri().toURL().openStream(), maxWordsPerLine, linesPerTime);
    }

    public TextFileSentenceGenerator(String resourceName, int maxWordsPerLine) throws IOException {
        this(resourceName, maxWordsPerLine, 1);
    }

    public TextFileSentenceGenerator(String resourceName, int maxWordsPerLine, int linesPerTime)
            throws IOException {

        this(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName),
                maxWordsPerLine,
                linesPerTime);
    }

    private TextFileSentenceGenerator(InputStream is, int maxWordsPerLine, int linesPerTime)
            throws IOException {
        lines = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (is) {
            String line;
            try (BufferedReader br = new BufferedReader(new java.io.InputStreamReader(is))) {
                while ((line = br.readLine()) != null) {
                    line = StringUtils.stripAccents(line.strip());
                    if (line.isEmpty()) {
                        add(lines, builder.toString().strip(), maxWordsPerLine);
                        builder.delete(0, builder.length());
                    } else {
                        String stripped = line.replaceAll("[^\\w\\s]+", " ").toLowerCase().strip();
                        builder.append(stripped).append(" ");
                    }
                }
            }
        }
        String remaining = builder.toString().strip();
        if (!remaining.isEmpty()) {
            add(lines, remaining, maxWordsPerLine);
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("No lines available.");
        }
        System.out.println("Number of unique lines: " + lines.size());
        this.count = 0;
        this.linesPerTime = linesPerTime;
    }

    private static void add(List<String> list, String line, int maxWordsPerLine) {
        line = line.strip();
        if (line.isEmpty()) {
            list.add("");
            return;
        }
        String[] words = line.split("\\s+");
        for (int i = 0; i < words.length; i += maxWordsPerLine) {
            StringBuilder builder = new StringBuilder();
            for (int j = i; j < i + maxWordsPerLine && j < words.length; j++) {
                builder.append(words[j]).append(" ");
            }
            list.add(builder.toString().strip());
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    private String nextLine() {
        // discard punctuation
        String line = lines.get(count);
        count += 1;
        if (count == lines.size()) {
            count = 0;
        }
        return line;
    }

    @Override
    public String next() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < linesPerTime; i++) {
            builder.append(nextLine());
            if (i > 0) {
                builder.append(" ");
            }
        }
        return builder.toString();
    }

    public static void main(String[] args) throws IOException {
        TextFileSentenceGenerator supplier = new TextFileSentenceGenerator("pg135.txt", 30);
        for (int i = 0; i < 100; i++) {
            System.out.println(supplier.next());
        }
    }
}
