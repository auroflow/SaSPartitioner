package cn.edu.zju.daily.metricflux.core.source;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import cn.edu.zju.daily.metricflux.core.data.Record;
import java.net.Socket;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TimestampedSocketRecordFunction<K, R extends Record<K>>
        implements SourceFunction<R> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(TimestampedSocketRecordFunction.class);

    /** Default delay between successive connection attempts. */
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

    /** Default connection timeout when connecting to the server socket (infinite). */
    protected static final int CONNECTION_TIMEOUT_TIME = 0;

    protected final String hostname;
    protected final int port;
    protected final long maxNumRetries;
    protected final long delayBetweenRetries;

    private transient Socket currentSocket;

    protected volatile boolean isRunning = true;

    public TimestampedSocketRecordFunction(String hostname, int port) {
        this(hostname, port, 0, DEFAULT_CONNECTION_RETRY_SLEEP);
    }

    public TimestampedSocketRecordFunction(String hostname, int port, long maxNumRetries) {
        this(hostname, port, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
    }

    public TimestampedSocketRecordFunction(
            String hostname, int port, long maxNumRetries, long delayBetweenRetries) {
        checkArgument(isValidClientPort(port), "port is out of range");
        checkArgument(
                maxNumRetries >= -1,
                "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");

        this.hostname = checkNotNull(hostname, "hostname must not be null");
        this.port = port;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    @Override
    public void cancel() {
        isRunning = false;

        // we need to close the socket as well, because the Thread.interrupt() function will
        // not wake the thread in the socketStream.read() method when blocked.
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }
}
