package cn.edu.zju.daily.metricflux.task.tdigest.source;

import cn.edu.zju.daily.metricflux.core.source.TimestampedSocketRecordFunction;
import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestRecord;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimestampedTDigestSocketRecordFunction
        extends TimestampedSocketRecordFunction<Integer, TDigestRecord> {

    private final int numValues;

    public TimestampedTDigestSocketRecordFunction(String hostname, int port, int numValues) {
        super(hostname, port);
        this.numValues = numValues;
    }

    public TimestampedTDigestSocketRecordFunction(
            String hostname, int port, String delimiter, long maxNumRetries, int numValues) {
        super(hostname, port, maxNumRetries);
        this.numValues = numValues;
    }

    public TimestampedTDigestSocketRecordFunction(
            String hostname,
            int port,
            long maxNumRetries,
            long delayBetweenRetries,
            int numValues) {
        super(hostname, port, maxNumRetries, delayBetweenRetries);
        this.numValues = numValues;
    }

    @Override
    public void run(SourceContext<TDigestRecord> ctx) throws Exception {
        long attempt = 0;
        int capacity = Integer.BYTES + numValues * Double.BYTES;
        byte[] buffer = new byte[capacity];

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
                LOG.info("Connected to server socket " + hostname + ':' + port);
                DataInputStream in =
                        new DataInputStream(new BufferedInputStream((socket.getInputStream())));

                while (isRunning) {
                    int key = in.readInt();
                    int valueSize = in.readInt();
                    double[] values = new double[valueSize];
                    for (int i = 0; i < valueSize; i++) {
                        values[i] = in.readDouble();
                    }
                    long timestamp = System.currentTimeMillis();
                    ctx.collectWithTimestamp(new TDigestRecord(key, timestamp, values), timestamp);
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
    }
}
