package io.conduktor.gateway.soak.func;

import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final String TENANT = "someTenant";
    private static final String USERNAME = "someUser";
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private static final String KAFKA_TOPIC = "test_topic";
    private static final String DOCKER_COMPOSE_FILE_PATH = "/docker-compose.yaml";
    private static final String DOCKER_COMPOSE_FOLDER_PATH = "/docker-compose/";
    private static DockerComposeContainer<?> composeContainer;
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    private static File dockerFile;
    private static List<Arguments> scenarios;

    private ClientFactory clientFactory;

    @BeforeAll
    public void setUp() throws IOException {
        clientFactory = generateClientFactory(this.getClass().getName());
        loadScenarios();
        dockerFile = new File(Objects.requireNonNull(ScenarioTest.class.getResource(DOCKER_COMPOSE_FILE_PATH)).getFile());

    }

    private static Stream<Arguments> sourceForScenario() {
        return scenarios.stream();
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(Scenario scenario) throws IOException, InterruptedException, ExecutionException {
        var kafka = scenario.getKafka();
        var gateway = scenario.getGateway();
        var plugins = scenario.getPlugins();
        var workflow = scenario.getWorkflow();

        startContainer(kafka, gateway);
        //Configure plugins
//        for(var plugin : plugins.entrySet()) {
//            var pluginName = plugin.getKey();
//            var pluginBody = plugin.getValue();
//            var response = given()
//                    .baseUri("http://localhost:8888/admin/interceptors/v1")
//                    .auth()
//                    .basic(ADMIN_USER, ADMIN_PASSWORD)
//                    .body(pluginBody)
//                    .contentType(ContentType.JSON)
//                    .when()
//                    .post("/tenants/{tenant}/users/{username}/interceptors/{pluginName}", TENANT, USERNAME, pluginName)
//                    .then()
//                    .statusCode(404)
//                    .extract()
//                    .response();
//            System.out.println(response);
//        }
        var topic = UUID.randomUUID().toString();
        try (var adminClient = clientFactory.kafkaAdmin()) {
            KafkaActionUtils.createTopic(adminClient, topic, 1, (short) 1, Map.of(), 10);
        }

        try (var producer = clientFactory.gatewayProducer()) {
            var value = "value1";
            KafkaActionUtils.produce(producer, topic, value);
        }
        try (var consumer = clientFactory.gatewayConsumer("groupId")) {
            var records = KafkaActionUtils.consume(consumer, topic, 1);
            var recordHeaders = records.iterator().next().headers().toArray();
        }

        //Do produce, consume, assert


        stopContainer();
    }

    private static void stopContainer() {
        composeContainer.stop();
    }

    private void startContainer(Scenario.Service kafka, Scenario.Service gateway) throws IOException {
        var tempComposeFile = getUpdatedDockerComposeFile(kafka.getEnvironment(), gateway.getEnvironment());
        composeContainer = new DockerComposeContainer<>(tempComposeFile)
                .withEnv("CP_VERSION", kafka.getVersion())
                .withEnv("GATEWAY_VERSION", gateway.getVersion())
                .waitingFor("kafka1", new KafkaTopicsWaitStrategy(9092))
                .waitingFor("kafka2", new KafkaTopicsWaitStrategy(9093))
                .waitingFor("schema-registry", Wait.forHttp("/subjects").forStatusCode(200))
                .waitingFor("conduktor-gateway",Wait.forListeningPort())
        ;
        try {
            composeContainer.start();
        } catch (Throwable e) {
            log.error("Start docker failed", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void loadScenarios() throws IOException {
        var folder = new File(SCENARIO_DIRECTORY_PATH);
        scenarios = new ArrayList<>();
        if (folder.exists() && folder.isDirectory()) {
            var files = folder.listFiles();
            if (files != null) {
                var configReader = YamlConfigReader.forType(Scenario.class);
                for (var file : files) {
                    if (file.isFile() && file.getName().toLowerCase().endsWith(".yaml")) {
                        var scenario = configReader.readYamlInResources(file.getPath());
                        scenarios.add(Arguments.of(scenario));
                    }
                }
            }
        } else {
            log.error("The specified folder does not exist or is not a directory.");
        }
    }

    @NotNull
    private File getUpdatedDockerComposeFile(LinkedHashMap<String, String> kafkaConfigs, LinkedHashMap<String, String> gatewayConfigs) throws IOException {
        var yaml = new Yaml();
        var composeConfig = yaml.load(ScenarioTest.class.getResourceAsStream(DOCKER_COMPOSE_FILE_PATH));

        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "kafka1", kafkaConfigs);
        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "conduktor-gateway", gatewayConfigs);

        // Save the modified composeConfig to a new file
        var tempComposeFile = new File(ScenarioTest.class.getResource(DOCKER_COMPOSE_FOLDER_PATH).getPath() + "docker-compose.yaml");
        try (var writer = new FileWriter(tempComposeFile)) {
            yaml.dump(composeConfig, writer);
        }
        return tempComposeFile;
    }

    private void appendEnvironments(LinkedHashMap<String, Object> composeConfig, String serviceName, LinkedHashMap<String, String> configs) {
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

    public ClientFactory generateClientFactory(String clientId) {
        return new ClientFactory(getGatewayProperties(clientId), getKafkaProperties());
    }
    private Properties getGatewayProperties(String clientId) {
        Properties clientProperties = new Properties();
        clientProperties.put("bootstrap.servers", "localhost:6969");
        clientProperties.put("sasl.mechanism", "PLAIN");
        clientProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        clientProperties.put("client.id", clientId);
        return clientProperties;
    }

    private Properties getKafkaProperties() {
        Properties clientProperties = new Properties();
        clientProperties.put("bootstrap.servers", "localhost:9092");
        return clientProperties;
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
