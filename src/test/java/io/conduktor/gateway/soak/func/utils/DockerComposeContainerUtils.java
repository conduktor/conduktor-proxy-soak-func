package io.conduktor.gateway.soak.func.utils;

import io.conduktor.gateway.soak.func.ScenarioTest;
import io.conduktor.gateway.soak.func.config.Scenario;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

@Slf4j
@Getter
public class DockerComposeContainerUtils {

    private static final String DOCKER_COMPOSE_FILE_PATH = "/docker-compose.yaml";
    private static final String DOCKER_COMPOSE_FOLDER_PATH = "/docker-compose/";
    public static final String GATEWAY_SECURITY_PROTOCOL = "GATEWAY_SECURITY_PROTOCOL";
    private static DockerComposeContainer<?> composeContainer;

//    public static int ZK_PORT;
//    public static int KAFKA1_INTERNAL_PORT;
//
//    public static int KAFKA1_EXTERNAL_PORT;
//
//    public static int KAFKA2_INTERNAL_PORT;
//
//    public static int KAFKA2_EXTERNAL_PORT;
//    public static int SCHEMA_REGISTRY_PORT;
//    public static String GATEWAY_PORT_RANGE;
//    public static int GATEWAY_PORT;
//    public static int GATEWAY_PUBLIC_PORT;


    public static void stopContainer() {
        composeContainer.stop();
    }

    public static void startContainer(Scenario.Service kafka, Scenario.Service gateway) throws IOException {
//        ZK_PORT = PortHelper.getFreePort();
//        KAFKA1_INTERNAL_PORT = PortHelper.getFreePort();
//        KAFKA1_EXTERNAL_PORT = PortHelper.getFreePort();
//        KAFKA2_INTERNAL_PORT = PortHelper.getFreePort();
//        KAFKA2_EXTERNAL_PORT = PortHelper.getFreePort();
//        SCHEMA_REGISTRY_PORT = PortHelper.getFreePort();
//        GATEWAY_PUBLIC_PORT = PortHelper.getFreePort();
//        var ranges = PortHelper.getContinuousFreePort(7);
//        GATEWAY_PORT_RANGE = ranges.get(0) + ":" + ranges.get(ranges.size() - 1);
//        GATEWAY_PORT = ranges.get(0);
        updateGatewayForTLS(gateway);
        var tempComposeFile = getUpdatedDockerComposeFile(kafka.getEnvironment(), gateway.getEnvironment());
        composeContainer = new DockerComposeContainer<>(tempComposeFile)
                .withEnv("CP_VERSION", kafka.getVersion())
                .withEnv("GATEWAY_VERSION", gateway.getVersion())
//                .withEnv("ZK_PORT", String.valueOf(ZK_PORT))
//                .withEnv("KAFKA1_INTERNAL_PORT", String.valueOf(KAFKA1_INTERNAL_PORT))
//                .withEnv("KAFKA1_EXTERNAL_PORT", String.valueOf(KAFKA1_EXTERNAL_PORT))
//                .withEnv("KAFKA2_INTERNAL_PORT", String.valueOf(KAFKA2_INTERNAL_PORT))
//                .withEnv("KAFKA2_EXTERNAL_PORT", String.valueOf(KAFKA2_EXTERNAL_PORT))
//                .withEnv("SCHEMA_REGISTRY_PORT", String.valueOf(SCHEMA_REGISTRY_PORT))
//                .withEnv("GATEWAY_PORT_RANGE", GATEWAY_PORT_RANGE)
//                .withEnv("GATEWAY_PORT", String.valueOf(GATEWAY_PORT))
//                .withEnv("GATEWAY_PUBLIC_PORT", String.valueOf(GATEWAY_PUBLIC_PORT))
                .waitingFor("kafka1", new KafkaTopicsWaitStrategy(9092))
                .waitingFor("kafka2", new KafkaTopicsWaitStrategy(9093))
                .waitingFor("schema-registry", Wait.forHttp("/subjects").forStatusCode(200))
                .waitingFor("conduktor-gateway", Wait.forListeningPort())
        ;
        try {
            composeContainer.start();
            log.info("Docker started");
        } catch (Throwable e) {
            log.error("Start docker failed", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void updateGatewayForTLS(Scenario.Service gateway) {
        if (gateway.getEnvironment().get(GATEWAY_SECURITY_PROTOCOL).equals(SASL_SSL.name())) {
            gateway.getEnvironment().put("GATEWAY_SSL_KEY_STORE_PATH", "./config/tls/sasl-ssl/keystore.jks");
        } else if (gateway.getEnvironment().get(GATEWAY_SECURITY_PROTOCOL).equals(SSL.name())) {
            gateway.getEnvironment().put("GATEWAY_SSL_KEY_STORE_PATH", "./config/tls/ssl/keystore.jks");
        }
    }

    @NotNull
    private static File getUpdatedDockerComposeFile(LinkedHashMap<String, String> kafkaConfigs, LinkedHashMap<String, String> gatewayConfigs) throws IOException {
        var yaml = new Yaml();
        var composeConfig = yaml.load(ScenarioTest.class.getResourceAsStream(DOCKER_COMPOSE_FILE_PATH));

        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "kafka1", kafkaConfigs);
        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "kafka2", kafkaConfigs);
        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "conduktor-gateway", gatewayConfigs);

        // Save the modified composeConfig to a new file
        var tempComposeFile = new File(ScenarioTest.class.getResource(DOCKER_COMPOSE_FOLDER_PATH).getPath() + "docker-compose.yaml");
        try (var writer = new FileWriter(tempComposeFile)) {
            yaml.dump(composeConfig, writer);
        }
        return tempComposeFile;
    }

    private static void appendEnvironments(LinkedHashMap<String, Object> composeConfig, String serviceName, LinkedHashMap<String, String> configs) {
        // Update the environment variables for the specified service
        var services = (Map<String, Object>) composeConfig.get("services");
        if (services.containsKey(serviceName)) {
            var serviceConfig = (Map<String, Object>) services.get(serviceName);
            var environment = (Map<String, String>) serviceConfig.computeIfAbsent("environment", k -> new LinkedHashMap<>());
            environment.putAll(configs);
        } else {
            throw new IllegalArgumentException("Service '" + serviceName + "' not found in the docker-compose.yml file.");
        }
    }


    public static class KafkaTopicsWaitStrategy implements WaitStrategy {

        private int listenerPort;
        private Duration timeout = Duration.ofSeconds(60);

        public KafkaTopicsWaitStrategy(int listenerPort) {
            this.listenerPort = listenerPort;
        }

        @Override
        public void waitUntilReady(WaitStrategyTarget target) {
            long startTime = System.currentTimeMillis();
            while (true) {
                try {
                    if (target.execInContainer(
                            "timeout",
                            "10",
                            "kafka-topics",
                            "--bootstrap-server",
                            "localhost:" + listenerPort,
                            "--list").getExitCode() == 0) {
                        break;
                    }
                } catch (Exception e) {
                    // ignored
                    break;
                }
                if (System.currentTimeMillis() > startTime + timeout.toMillis()) {
                    break;
                }
            }
        }

        @Override
        public WaitStrategy withStartupTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

    }
}
