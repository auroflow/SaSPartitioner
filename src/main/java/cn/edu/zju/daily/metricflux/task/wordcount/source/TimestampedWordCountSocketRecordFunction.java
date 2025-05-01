package cn.edu.zju.daily.metricflux.task.wordcount.source;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.wordcount.data.WordCountRecord;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimestampedWordCountSocketRecordFunction
        extends TimestampedSocketRecordFunction<Integer, WordCountRecord> {

    private final String delimiter;

    public TimestampedWordCountSocketRecordFunction(String hostname, int port) {
        super(hostname, port);
        this.delimiter = "\n";
    }

    public TimestampedWordCountSocketRecordFunction(String hostname, int port, long maxNumRetries) {
        super(hostname, port, maxNumRetries);
        this.delimiter = "\n";
    }

    public TimestampedWordCountSocketRecordFunction(
            String hostname,
            int port,
            String delimiter,
            long maxNumRetries,
            long delayBetweenRetries) {
        super(hostname, port, maxNumRetries, delayBetweenRetries);
        this.delimiter = delimiter;
    }

    @Override
    public void run(SourceContext<WordCountRecord> ctx) throws Exception {
        final StringBuilder buffer = new StringBuilder();
        long attempt = 0;

        while (isRunning) {

            try (Socket socket = new Socket()) {
                LOG.info("Connecting to server socket " + hostname + ':' + port);
                while (true) {
                    try {
                        socket.connect(
                                new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                        if (socket.isConnected()) {
                            break;
                        }
                    } catch (ConnectException ignored) {
                    }
                    LOG.info("Waiting for server socket to be available");
                    Thread.sleep(delayBetweenRetries);
                }
                try (BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    char[] cbuf = new char[8192];
                    int bytesRead;
                    while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                        buffer.append(cbuf, 0, bytesRead);
                        int delimPos;
                        while (buffer.length() >= delimiter.length()
                                && (delimPos = buffer.indexOf(delimiter)) != -1) {
                            String record = buffer.substring(0, delimPos);
                            // truncate trailing carriage return
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            long timestamp = System.currentTimeMillis();
                            ctx.collectWithTimestamp(strToRecord(record, timestamp), timestamp);
                            buffer.delete(0, delimPos + delimiter.length());
                        }
                    }
                }
            }

            // if we dropped out of this loop due to an EOF, sleep and retry
            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    LOG.warn(
                            "Lost connection to server socket. Retrying in "
                                    + delayBetweenRetries
                                    + " msecs...");
                    Thread.sleep(delayBetweenRetries);
                } else {
                    // this should probably be here, but some examples expect simple exists of the
                    // stream source
                    // throw new EOFException("Reached end of stream and reconnects are not
                    // enabled.");
                    break;
                }
            }
        }

        // collect trailing data
        if (buffer.length() > 0) {
            long timestamp = System.currentTimeMillis();
            try {
                ctx.collectWithTimestamp(strToRecord(buffer.toString(), timestamp), timestamp);
            } catch (Exception ignored) {
            }
        }
    }

    private WordCountRecord strToRecord(String str, long timestamp) {
        // string to Record
        try {
            String[] attrs = str.split(",", 2); // split only once
            return new WordCountRecord(Integer.parseInt(attrs[0]), timestamp, attrs[1]);
        } catch (Exception e) {
            LOG.error("Error parsing record: " + str, e);
            throw e;
        }
    }
}
