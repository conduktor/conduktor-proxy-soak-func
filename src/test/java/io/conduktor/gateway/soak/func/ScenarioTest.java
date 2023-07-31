package io.conduktor.gateway.soak.func;

import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import lombok.extern.slf4j.Slf4j;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.ContainerUtils.startContainer;
import static io.conduktor.gateway.soak.func.utils.ContainerUtils.stopContainer;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final String TENANT = "someTenant";
    private static final String USERNAME = "someUser";
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private static final String KAFKA_TOPIC = "test_topic";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    private static List<Arguments> scenarios;

    private ClientFactory clientFactory;

    @BeforeAll
    public void setUp() throws IOException {
        clientFactory = ClientFactory.generateClientFactory(this.getClass().getName());
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
    public void testScenario(Scenario scenario) throws IOException, InterruptedException, ExecutionException {
        var kafka = scenario.getKafka();
        var gateway = scenario.getGateway();
        var plugins = scenario.getPlugins();
        var input = scenario.getIo().getInput();
        var output = scenario.getIo().getOutput();

        startContainer(kafka, gateway);
        //TODO: Configure plugins
//        for (var plugin : plugins.entrySet()) {
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
//                    .statusCode(200)
//                    .extract()
//                    .response();
//            System.out.println(response);
//        }
        var topic = UUID.randomUUID().toString();
        try (var adminClient = clientFactory.gatewayAdmin()) {
            KafkaActionUtils.createTopic(adminClient, topic, 1, (short) 1, Map.of(), 10);
        }

        var inputHeaders = new ArrayList<Header>();
        var inputKey = input.getKey();
        var inputValue = input.getValue();
        try (var producer = clientFactory.gatewayProducer()) {
            for(var header : input.getHeaders().entrySet()) {
                inputHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
            }
            KafkaActionUtils.produce(producer, topic, inputKey, inputValue, inputHeaders);
        }
        try (var consumer = clientFactory.gatewayConsumer("groupId")) {
            var records = KafkaActionUtils.consume(consumer, topic, 1);
            var record = records.iterator().next();
            var consumedHeaders = record.headers().toArray();
            Assertions.assertThat(record.key()).isEqualTo(inputKey);
            Assertions.assertThat(record.value()).isEqualTo(inputValue);
            Assertions.assertThat(record.headers()).hasSize(consumedHeaders.length);
            Assertions.assertThat(record.headers()).contains(consumedHeaders);
        }

        stopContainer();
    }


}
