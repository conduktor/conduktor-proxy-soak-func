package io.conduktor.gateway.soak.func.utils;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.shaded.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class PortHelper {

    private static final HashSet<Integer> ACQUIRED_PORTS = new HashSet<>();

    private static final int PORT_RETRIES = 50;

    public static void registerPort(int port) {
        ACQUIRED_PORTS.add(port);
    }

    public static void registerPorts(List<Integer> port) {
        ACQUIRED_PORTS.addAll(port);
    }

    public static boolean isRegistered(int port) { return ACQUIRED_PORTS.contains(port); }

    public static boolean isFreePort(int port) {
        return !isRegistered(port) && isOSFreePort(port);
    }

    @VisibleForTesting
    static boolean isOSFreePort(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static int getFreePort() {
        return getContinuousFreePort(1).get(0);
    }

    public static List<Integer> getContinuousFreePort(int numPorts) {
        for (int attempt = 0; attempt < PORT_RETRIES; attempt++) {
            try (ServerSocket firstSocket = new ServerSocket(0)) {
                int firstPort = firstSocket.getLocalPort();
                if (!isRegistered(firstPort)) {
                    var ports = new ArrayList<Integer>();
                    ports.add(firstPort);
                    for (int nextPort = firstPort + 1; ports.size() < numPorts && isFreePort(nextPort); nextPort++) {
                        ports.add(nextPort);
                    }
                    if (ports.size() == numPorts) {
                        registerPorts(ports);
                        return ports;
                    }
                }
            } catch (IOException e) {
                log.warn("Cannot get a random first port ({}), retry {}", e, attempt);
            }
        }
        log.error("Cannot get port range of {} ports, no more attempts {}", numPorts, PORT_RETRIES);
        throw new RuntimeException("Cannot get port range of " + numPorts + ", no more attempts " + PORT_RETRIES);
    }


}
