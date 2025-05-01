package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import cn.edu.zju.daily.metricflux.task.tdigest.data.TDigestRecord;
import cn.edu.zju.daily.metricflux.utils.rate.FixedRateLimiter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class TDigestSocketServerTest {

    private final int numValues = 100;
    private final int port = 9999;

    /** Connects to the server and receives data. */
    class TDigestTestReceiver implements Runnable {

        private final int port;
        private boolean stopped = false;

        public TDigestTestReceiver(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try (Socket socket = new Socket("localhost", port)) {
                System.out.println("TDigestTestReceiver: connection accepted.");
                DataInputStream in =
                        new DataInputStream(new BufferedInputStream(socket.getInputStream()));

                double start = System.currentTimeMillis();
                int count = 0;

                while (true) {
                    try {
                        int key = in.readInt();
                        int valueSize = in.readInt();

                        double[] values = new double[valueSize];
                        for (int i = 0; i < valueSize; i++) {
                            values[i] = in.readDouble();
                        }
                        long timestamp = System.currentTimeMillis();
                        TDigestRecord record = new TDigestRecord(key, timestamp, values);
                        count++;
                    } catch (IOException e) {
                        break;
                    }
                }
                double end = System.currentTimeMillis();
                System.out.println("Received " + count + " records in " + (end - start) + " ms");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void stop() {
            stopped = true;
        }
    }

    @Test
    void test() throws IOException {
        TDigestDatasetSupplier datasetSupplier =
                new TDigestDatasetSupplier("/home/user/code/data/london_smart_meters.csv", 10000);
        Iterator<Integer> keySupplier = datasetSupplier.getKeySupplier();
        TDigestValueSupplier valueSupplier = datasetSupplier.getValueSupplier();
        TDigestSocketServer server =
                new TDigestSocketServer(
                        "localhost",
                        port,
                        keySupplier,
                        valueSupplier,
                        new FixedRateLimiter(0),
                        Integer.BYTES + datasetSupplier.getMaxValueSize() * Double.BYTES);
        server.startAsync();

        // Test how many records are sent in 1 second
        TDigestTestReceiver receiver = new TDigestTestReceiver(port);
        receiver.run();

        server.stop();
    }
}
