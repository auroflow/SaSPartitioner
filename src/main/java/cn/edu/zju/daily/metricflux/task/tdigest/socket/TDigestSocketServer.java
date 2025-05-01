package cn.edu.zju.daily.metricflux.task.tdigest.socket;

import cn.edu.zju.daily.metricflux.core.socket.SocketServer;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDigestSocketServer implements SocketServer {
    private final String ip;
    private final int port;
    private final Iterator<Integer> keySupplier;
    private final TDigestValueSupplier valueSupplier;
    private final RateLimiter rateLimiter;
    private final int valueBufferCapacity;
    private boolean stopped;

    /**
     * SocketServer for TDigest
     *
     * @param ip IP
     * @param port port
     * @param keySupplier key supplier
     * @param valueSupplier value supplier
     * @param rateLimiter rate limiter
     * @param valueBufferCapacity value buffer capacity in bytes
     */
    public TDigestSocketServer(
            String ip,
            int port,
            Iterator<Integer> keySupplier,
            TDigestValueSupplier valueSupplier,
            RateLimiter rateLimiter,
            int valueBufferCapacity) {
        this.ip = ip;
        this.port = port;
        this.keySupplier = keySupplier;
        this.valueSupplier = valueSupplier;
        this.rateLimiter = rateLimiter;
        this.valueBufferCapacity = valueBufferCapacity;
    }

    public TDigestSocketServer(
            int port,
            Iterator<Integer> keySupplier,
            TDigestValueSupplier valueSupplier,
            RateLimiter rateLimiter,
            int valueBufferCapacity) {
        this("", port, keySupplier, valueSupplier, rateLimiter, valueBufferCapacity);
    }

    public void start() {
        long count = 0;
        InetAddress address = null;
        if (!ip.isEmpty()) {
            try {
                address = InetAddress.getByName(ip);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        try {
            ServerSocket serverSocket = new ServerSocket(port, 1, address);
            Socket socket = serverSocket.accept();
            System.out.println("TextSocketServer: connection accepted.");
            DataOutputStream out =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            long nextEmitNanoTime = System.nanoTime();
            while (!stopped && keySupplier.hasNext()) {
                long delay = rateLimiter.getDelayNanos(count);
                nextEmitNanoTime += delay;
                while (System.nanoTime() < nextEmitNanoTime) {
                    continue;
                }

                out.writeInt(keySupplier.next());
                valueSupplier.supply(out);
                count++;
            }

            out.flush();
            socket.close();
            serverSocket.close();
        } catch (SocketException e) {
            LOG.info("SocketException: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void startAsync() {
        new Thread(this::start).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
