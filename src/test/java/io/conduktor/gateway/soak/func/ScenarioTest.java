package io.conduktor.gateway.soak.func;

import com.auth0.jwt.JWT;
import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
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
import java.util.regex.Pattern;
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
    public static final String PASS_THROUGH_TENANT = "passThroughTenant";
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
    public void testScenario(Scenario scenario) throws IOException, InterruptedException, ExecutionException {
        log.info("Start to test scenario: {}", scenario);
        var kafka = scenario.getKafka();
        var gateway = scenario.getGateway();
        var plugins = scenario.getPlugins();
        var gatewayInput = scenario.getIo().getInput();
        var kafkaOutput = scenario.getIo().getKafka();
        var gatewayOutput = scenario.getIo().getOutput();

        // Start container
        startContainer(kafka, gateway);

        // Extract tenant if it presents
        String tenant = getTenant(gateway.getProperties());

        // Configure plugins
        if (Objects.nonNull(plugins) && StringUtils.isNotEmpty(tenant)) {
            configurePlugins(plugins, tenant);
        }

        var clientFactory = ClientFactory.generateClientFactory(this.getClass().getName(), gateway.getProperties());
        var topic = UUID.randomUUID().toString();
        // Create topic
        try (var adminClient = clientFactory.gatewayAdmin()) {
            KafkaActionUtils.createTopic(adminClient, topic, 1, (short) 1, Map.of(), 10);
        }

        // Produce input into topic
        var inputHeaders = new ArrayList<Header>();
        try (var producer = clientFactory.gatewayProducer()) {
            for (var header : gatewayInput.getHeaders().entrySet()) {
                inputHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
            }
            KafkaActionUtils.produce(producer, topic, gatewayInput.getKey(), gatewayInput.getValue(), inputHeaders);
        }

        // Fetch message from real kafka
        try (var consumer = clientFactory.kafkaConsumer("someGroup")) {
            var records = KafkaActionUtils.consume(consumer, tenant + topic, 1);
            assertRecord(kafkaOutput, records);
        }

        // Fetch message from gateway
        try (var consumer = clientFactory.gatewayConsumer("groupId")) {
            var records = KafkaActionUtils.consume(consumer, topic, 1);
            assertRecord(gatewayOutput, records);
        }

        // Stop container
        clientFactory.close();
        stopContainer();
    }

    private static void configurePlugins(LinkedHashMap<String, PluginRequest> plugins, String tenant) {
        for (var plugin : plugins.entrySet()) {
            var pluginName = plugin.getKey();
            var pluginBody = plugin.getValue();
            var response = given()
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
            System.out.println(response);
        }
    }

    private static void assertRecord(Scenario.Record output, List<ConsumerRecord<String, String>> records) {
        var record = records.iterator().next();
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

    private static String getTenant(Map<String, String> gatewayProperties) {
        if (Objects.nonNull(gatewayProperties) && gatewayProperties.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            String property = gatewayProperties.get(SaslConfigs.SASL_JAAS_CONFIG);
            // Regular expression pattern to match the password
            var pattern = Pattern.compile("password=\\\"([^\"]+)\\\"");
            var matcher = pattern.matcher(property);
            if (matcher.find()) {
                var rawPassword = matcher.group(1);
                var decodedPassword = JWT.decode(rawPassword);
                if (Objects.nonNull(decodedPassword.getClaim("tenant"))) {
                    return decodedPassword.getClaim("tenant").asString();
                }
            }
        }
        return PASS_THROUGH_TENANT;
    }


}
