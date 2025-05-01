package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Reads CSV in key,text format. */
public class CSVTextSupplier implements Iterator<String> {

    private final List<String> lines;
    private long count;
    private final long maxLines;
    private int index;

    public CSVTextSupplier(Path path, long maxLines) throws IOException {
        this(path.toUri().toURL().openStream(), maxLines);
    }

    public CSVTextSupplier(String resourceName, long maxLines) throws IOException {
        this(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName),
                maxLines);
    }

    private CSVTextSupplier(InputStream is, long maxLines) throws IOException {
        this.lines = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (is) {
            String line;
            try (BufferedReader br = new BufferedReader(new java.io.InputStreamReader(is))) {
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }
        }
        this.maxLines = maxLines;
        this.count = 0;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return count < maxLines;
    }

    private String nextLine() {
        if (count >= maxLines) {
            throw new NoSuchElementException();
        }
        count++;

        String line = lines.get(index);
        index += 1;
        if (index == lines.size()) {
            index = 0;
        }
        return line;
    }

    @Override
    public String next() {
        return nextLine();
    }

    public static void main(String[] args) throws IOException {
        CSVTextSupplier supplier =
                new CSVTextSupplier(Path.of("/home/user/code/data/t4sa.csv"), 100);
        while (supplier.hasNext()) {
            System.out.println(supplier.next());
        }
    }
}
