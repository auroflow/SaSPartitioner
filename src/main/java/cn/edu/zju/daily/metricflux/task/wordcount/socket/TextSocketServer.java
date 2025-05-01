package cn.edu.zju.daily.metricflux.task.wordcount.socket;

import cn.edu.zju.daily.metricflux.core.socket.SocketServer;
import cn.edu.zju.daily.metricflux.utils.rate.RateLimiter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;

public class TextSocketServer implements SocketServer {

    private final String ip;
    private final int port;
    private final Iterator<String> supplier; // key + "," + sentence
    private final RateLimiter rateLimiter;
    private boolean stopped;

    public TextSocketServer(
            String ip, int port, Iterator<String> supplier, RateLimiter rateLimiter) {
        this.ip = ip;
        this.port = port;
        this.supplier = supplier;
        this.rateLimiter = rateLimiter;
    }

    public TextSocketServer(int port, Iterator<String> supplier, RateLimiter rateLimiter) {
        this("", port, supplier, rateLimiter);
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
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

            long nextEmitNanoTime = System.nanoTime();
            while (!stopped && supplier.hasNext()) {
                long delay = rateLimiter.getDelayNanos(count);
                nextEmitNanoTime += delay;
                while (System.nanoTime() < nextEmitNanoTime) {
                    continue;
                }
                String line = supplier.next().strip();
                if (!line.contains(",")) {
                    System.out.println("ERROR: malformed line " + line);
                }
                writer.println(line);
                count++;
            }

            socket.getOutputStream().flush();
            socket.close();
            serverSocket.close();
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
