package io.conduktor.gateway.soak.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.AbstractAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.DockerComposeContainerUtils.startContainer;
import static io.conduktor.gateway.soak.func.utils.DockerComposeContainerUtils.stopContainer;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
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

    @AfterEach
    void cleanup() {
        stopContainer();
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(Scenario scenario) throws IOException, InterruptedException, ExecutionException {
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
            var recordAssertions = action.getAssertions();

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
                                consumeAndEvaluate(topic, consumer, recordAssertions);
                            }
                        }
                        case GATEWAY -> {
                            try (var consumer = clientFactory.gatewayConsumer("groupId", properties)) {
                                consumeAndEvaluate(topic, consumer, recordAssertions);
                            }
                        }
                    }
                }
            }
            clientFactory.close();
        }
    }

    private static void createTopic(String topic, AdminClient kafkaAdminClient) throws InterruptedException {
        KafkaActionUtils.createTopic(kafkaAdminClient, topic, 1, (short) 1, Map.of(), 10);
    }

    private static void produce(String topic, LinkedList<Scenario.Message> messages, KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        for (var message : messages) {
            var inputHeaders = new ArrayList<Header>();
            if (Objects.nonNull(message.getHeaders())) {
                for (var header : message.getHeaders().entrySet()) {
                    inputHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
                }
            }
            KafkaActionUtils.produce(producer, topic, message.getKey(), message.getValue(), inputHeaders);
        }
    }

    private static void consumeAndEvaluate(String topic, KafkaConsumer<String, String> consumer, List<Scenario.RecordAssertion> recordAssertions) {
        var records = KafkaActionUtils.consume(consumer, topic, recordAssertions.size());
        assertThat(records.size()).isEqualTo(recordAssertions.size());
        var i = 0;
        for (var record : records) {
            var assertion = recordAssertions.get(i++);
            if (StringUtils.isNotBlank(assertion.getDescription())) {
                log.info("Test: " + assertion.getDescription());
            }
            assertRecord(record, assertion);
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
                    .post("/vcluster/{tenant}/interceptor/{pluginName}", tenant, pluginName)
                    .then()
                    .statusCode(200)
                    .extract()
                    .response();
        }
    }

    private static void assertRecord(ConsumerRecord<String, String> record, Scenario.RecordAssertion recordAssertion) {
        assertData(record.key(), recordAssertion.getKey());
        assertData(record.value(), recordAssertion.getValue());
        if (Objects.nonNull(recordAssertion.getHeaders())) {
            var recordHeaders = IteratorUtils.toList(record.headers().iterator())
                    .stream()
                    .collect(Collectors.toMap(Header::key, Header::value));
            for (var entry : recordAssertion.getHeaders().entrySet()) {
                assertTrue(recordHeaders.containsKey(entry.getKey()));
                assertData(new String(recordHeaders.get(entry.getKey())), entry.getValue());
            }
        }
    }

    private static void assertData(String data, Scenario.Assertion assertion) {
        if (Objects.isNull(assertion)) {
            return;
        }
        var expected = assertion.getExpected();
        var assertMethod = assertion.getOperator();
        if (Objects.isNull(expected) || expected instanceof String str && StringUtils.isBlank(str)) {
            return;
        }
        try {
            if ("satisfies".equals(assertMethod)) {
                var dataAsMap = MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {
                });
                var parser = new SpelExpressionParser();
                var context = new StandardEvaluationContext(dataAsMap);
                context.setVariables(dataAsMap);

                var satisfied = parser.parseExpression(String.valueOf(expected)).getValue(context, Boolean.class);
                assertEquals(Boolean.TRUE, satisfied);
                return;
            }
            //TODO: support more types
            var method = AbstractAssert.class.getMethod(assertMethod, Object.class);
            method.invoke(assertThat(data), expected);
        } catch (NoSuchMethodException ex) {
            fail("Assertion.assertThat '" + assertMethod + "' is not supported");
        } catch (Throwable ex) {
            fail(ExceptionUtils.getRootCause(ex));
        }
    }


}
