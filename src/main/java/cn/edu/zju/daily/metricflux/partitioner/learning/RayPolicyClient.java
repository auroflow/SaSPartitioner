package cn.edu.zju.daily.metricflux.partitioner.learning;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

@Slf4j
public class RayPolicyClient<OBS, ACT> implements RLPolicyClient<OBS, ACT> {

    private Socket client;
    private final Pickler pickler = new Pickler();
    private final Unpickler unpickler = new Unpickler();
    private final byte[] intBuffer = new byte[Integer.BYTES];

    public RayPolicyClient(String rayServerHost, int rayServerPort) throws IOException {
        while (!tryConnect(rayServerHost, rayServerPort)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean tryConnect(String rayServerHost, int rayServerPort) {
        try {
            this.client = new Socket(rayServerHost, rayServerPort);
            LOG.info("Connected to Ray server {}:{}.", rayServerHost, rayServerPort);
            return true;
        } catch (IOException e) {
            LOG.error(
                    "Cannot connect to Ray server {}:{}: {}",
                    rayServerHost,
                    rayServerPort,
                    e.getMessage());
            return false;
        }
    }

    @Override
    public double[] getNextAction(double[] obs, double reward, Map<String, ?> info, boolean done) {
        try {
            LOG.debug("Sending request to Ray server...");
            sendRequest(Command.GET_NEXT_ACTION, obs, reward, info, done);
            LOG.debug("Sent request to Ray server.");
            double[] action = (double[]) readResponse().get("action");
            LOG.info("Received next action from Ray server.");
            return action;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendRequest(
            Command command, double[] obs, double reward, Map<String, ?> info, boolean done)
            throws IOException {
        Map<String, Object> data = new HashMap<>();
        data.put("cmd", command.ordinal());
        data.put("obs", obs);
        data.put("reward", reward);
        data.put("info", info);
        data.put("done", done);
        byte[] payload = pickler.dumps(data);
        sendBytes(payload);
    }

    private Map<String, Object> readResponse() throws IOException {
        byte[] response = readBytes();
        return parseResponse(response);
    }

    private void sendBytes(byte[] bytes) {
        try {
            _sendInt(bytes.length);
            client.getOutputStream().write(bytes);
            client.getOutputStream().flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readBytes() throws IOException {
        try {
            int length = _readNextInt();
            LOG.debug("Reading {} bytes", length);
            byte[] bytes = new byte[length];
            _readFullBytes(bytes, length);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int _readNextInt() throws IOException {
        _readFullBytes(intBuffer, Integer.BYTES);

        // Convert the byte array to an integer (big-endian).
        return (intBuffer[0] & 0xFF) << 24
                | (intBuffer[1] & 0xFF) << 16
                | (intBuffer[2] & 0xFF) << 8
                | (intBuffer[3] & 0xFF);
    }

    private void _readFullBytes(byte[] buffer, int length) throws IOException {
        if (buffer.length > length) {
            throw new IllegalArgumentException(
                    "Buffer length "
                            + buffer.length
                            + " smaller than specified length "
                            + length
                            + ".");
        }
        int bytesRead = 0;

        while (bytesRead < Integer.BYTES) {
            int delta = client.getInputStream().read(buffer, bytesRead, length - bytesRead);
            if (delta == -1) {
                throw new EOFException();
            }
            bytesRead += delta;
        }
    }

    private void _sendInt(int value) throws IOException {
        // Read an integer (big-endian).
        intBuffer[0] = (byte) (value >> 24);
        intBuffer[1] = (byte) (value >> 16);
        intBuffer[2] = (byte) (value >> 8);
        intBuffer[3] = (byte) value;
        client.getOutputStream().write(intBuffer);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseResponse(byte[] response) throws IOException {
        return (Map<String, Object>) unpickler.loads(response);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private enum Command {
        GET_NEXT_ACTION,
    }
}
