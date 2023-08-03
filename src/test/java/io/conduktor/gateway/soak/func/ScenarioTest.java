package io.conduktor.gateway.soak.func;

import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.DockerComposeContainerUtils.startContainer;
import static io.conduktor.gateway.soak.func.utils.DockerComposeContainerUtils.stopContainer;
import static io.restassured.RestAssured.given;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    private static List<Arguments> scenarios;

    @BeforeAll
    public void setUp() throws IOException {
        loadScenarios();

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

    private static Stream<Arguments> sourceForScenario() {
        return scenarios.stream();
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(Scenario scenario) {
        try {
            log.info("Start to test: {}", scenario.getTitle());
            var kafka = scenario.getDocker().getKafka();
            var gateway = scenario.getDocker().getGateway();
            var plugins = scenario.getPlugins();
            var actions = scenario.getActions();

            // Start container
            startContainer(kafka, gateway);

            // Configure plugins
            if (Objects.nonNull(plugins)) {
                configurePlugins(plugins);
            }

            // Perform actions
            for (var action : actions) {
                var type = action.getType();
                var target = action.getTarget();
                var properties = action.getProperties();
                var topic = action.getTopic();
                var messages = action.getMessages();
                var clientFactory = new ClientFactory();
                //TODO: refactor it to look better
                switch (type) {
                    case CREATE_TOPIC -> {
                        switch (target) {
                            case KAFKA -> {
                                try (var kafkaAdminClient = clientFactory.kafkaAdmin(properties)) {
                                    createTopic(topic, kafkaAdminClient);
                                }
                            }
                            case GATEWAY -> {
                                try (var gatewayAdminClient = clientFactory.gatewayAdmin(properties)) {
                                    createTopic(topic, gatewayAdminClient);
                                }
                            }
                        }
                    }
                    case PRODUCE -> {
                        switch (target) {
                            case KAFKA -> {
                                try (var kafkaProducer = clientFactory.kafkaProducer(properties)) {
                                    produce(topic, messages, kafkaProducer);
                                }
                            }
                            case GATEWAY -> {
                                try (var producer = clientFactory.gatewayProducer(properties)) {
                                    produce(topic, messages, producer);
                                }
                            }
                        }
                    }
                    case FETCH -> {
                        switch (target) {
                            case KAFKA -> {
                                try (var consumer = clientFactory.kafkaConsumer("groupId", properties)) {
                                    consumeAndEvaluate(topic, messages, consumer);
                                }
                            }
                            case GATEWAY -> {
                                try (var consumer = clientFactory.gatewayConsumer("groupId", properties)) {
                                    consumeAndEvaluate(topic, messages, consumer);
                                }
                            }
                        }
                    }
                }
                clientFactory.close();
            }
        } catch (Exception e) {
            log.error("Test failed", e);
            throw new RuntimeException(e.getMessage());
        } finally {
            stopContainer();
        }
    }

    private static void createTopic(String topic, AdminClient kafkaAdminClient) throws InterruptedException {
        KafkaActionUtils.createTopic(kafkaAdminClient, topic, 1, (short) 1, Map.of(), 10);
    }

    private static void produce(String topic, LinkedList<Scenario.Message> messages, KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        for (var message : messages) {
            var inputHeaders = new ArrayList<Header>();
            for (var header : message.getHeaders().entrySet()) {
                inputHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
            }
            KafkaActionUtils.produce(producer, topic, message.getKey(), message.getValue(), inputHeaders);
        }
    }

    private static void consumeAndEvaluate(String topic, LinkedList<Scenario.Message> messages, KafkaConsumer<String, String> consumer) {
        var records = KafkaActionUtils.consume(consumer, topic, messages.size());
        Assertions.assertThat(records.size()).isEqualTo(messages.size());
        int i = 0;
        for (var message : messages) {
            var record = records.get(i++);
            assertRecord(message, record);
        }
    }

    private static void configurePlugins(LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> plugins) {
        for (var plugin : plugins.entrySet()) {
            var tenant = plugin.getKey();
            configurePlugins(plugin.getValue(), tenant);
        }
    }

    private static void configurePlugins(LinkedHashMap<String, PluginRequest> plugins, String tenant) {
        for (var plugin : plugins.entrySet()) {
            var pluginName = plugin.getKey();
            var pluginBody = plugin.getValue();
            given()
                    .baseUri("http://localhost:8888/admin/interceptors/v1")
                    .auth()
                    .basic(ADMIN_USER, ADMIN_PASSWORD)
                    .body(pluginBody)
                    .contentType(ContentType.JSON)
                    .when()
                    .post("/tenants/{tenant}/interceptors/{pluginName}", tenant, pluginName)
                    .then()
                    .statusCode(200)
                    .extract()
                    .response();
        }
    }

    private static void assertRecord(Scenario.Message output, ConsumerRecord<String, String> record) {
        Assertions.assertThat(record.key()).isEqualTo(output.getKey());
        Assertions.assertThat(record.value()).isEqualTo(output.getValue());
        if (Objects.nonNull(output.getHeaders())) {
            Assertions.assertThat(record.headers()).hasSize(output.getHeaders().size());
            for (var outputHeader : output.getHeaders().entrySet()) {
                var header = new RecordHeader(outputHeader.getKey(), outputHeader.getValue().getBytes());
                Assertions.assertThat(record.headers()).contains(header);
            }
        }
    }


}
