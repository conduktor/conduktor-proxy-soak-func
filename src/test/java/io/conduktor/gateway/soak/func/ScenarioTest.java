package io.conduktor.gateway.soak.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.PluginResponse;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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

        var mapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID));

        // Perform actions
        for (var action : actions) {
            var type = action.getType();
            var clientFactory = new ClientFactory();

            log.info("Executing " + action.getType());

            switch (type) {
                case STEP -> {
                    var stepAction = ((Scenario.StepAction) action);
                    log.info(stepAction.description);
                }
                case DOCUMENTATION -> {
                    var documentationAction = ((Scenario.DocumentationAction) action);
                    log.debug(documentationAction.description);
                }
                case CREATE_TOPIC -> {
                    var createTopicAction = ((Scenario.CreateTopicAction) action);
                    var topic = createTopicAction.getTopic();
                    final Scenario.Service targetService = getTargetService(kafka, gateway, createTopicAction.getTarget());
                    log.info("Executing " + type + " on " + createTopicAction.getTarget());
                    final Properties properties = properties(targetService, createTopicAction.getProperties());
                    try (var kafkaAdminClient = clientFactory.kafkaAdmin(properties)) {
                        createTopic(topic, kafkaAdminClient);
                    }
                }
                case PRODUCE -> {
                    var produceAction = ((Scenario.ProduceAction) action);
                    var topic = produceAction.getTopic();
                    final Scenario.Service targetService = getTargetService(kafka, gateway, produceAction.getTarget());
                    log.info("Executing " + type + " on " + produceAction.getTarget());
                    final Properties properties = properties(targetService, produceAction.getProperties());
                    try (var kafkaProducer = clientFactory.kafkaProducer(properties)) {
                        produce(topic, produceAction.getMessages(), kafkaProducer);
                    }
                }
                case FETCH -> {
                    var fetchAction = ((Scenario.FetchAction) action);
                    var topic = fetchAction.getTopic();
                    final Scenario.Service targetService = getTargetService(kafka, gateway, fetchAction.getTarget());
                    log.info("Executing " + type + " on " + fetchAction.getTarget());
                    final Properties properties = properties(targetService, fetchAction.getProperties());
                    try (var consumer = clientFactory.consumer(properties)) {
                        consumeAndEvaluate(topic, consumer, fetchAction.getAssertions());
                    }
                }
                case ADD_INTERCEPTORS -> {
                    var addInterceptors = ((Scenario.AddInterceptorAction) action);
                    configurePlugins(addInterceptors.getInterceptors());
                }
                case REMOVE_INTERCEPTORS -> {
                    var removeInterceptors = ((Scenario.RemoveInterceptorAction) action);
                    for (String name : removeInterceptors.getNames()) {
                        removePlugin(removeInterceptors.vcluster, name);
                    }
                }
                case LIST_INTERCEPTORS -> {
                    var listInterceptors = ((Scenario.ListInterceptorAction) action);
                    TenantInterceptorsResponse response = getPlugins(listInterceptors.vcluster);

                    if (Objects.nonNull(listInterceptors.assertSize)) {
                        assertThat(response.interceptors)
                                .hasSize(listInterceptors.assertSize);
                    }
                    for (String assertion : listInterceptors.getAssertNames()) {
                        assertThat(response.interceptors)
                                .extracting(PluginResponse::getName)
                                .contains(assertion);
                    }
                }
            }
            clientFactory.close();
        }
    }

    private Scenario.Service getTargetService(Scenario.Service kafka, Scenario.Service gateway, Scenario.ActionTarget target) {
        switch (target) {
            case KAFKA:
                return kafka;
            case GATEWAY:
                return gateway;
            default:
                throw new RuntimeException(target + " is not supported");
        }
    }

    private Properties properties(Scenario.Service service, LinkedHashMap<String, String> properties) {
        Properties p = new Properties();
        if (service.getProperties() != null) {
            p.putAll(service.getProperties());
        }
        if (properties != null) {
            p.putAll(properties);
        }
        return p;
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
            configurePlugins(plugin.getValue(), plugin.getKey());
        }
    }


    private static void configurePlugins(LinkedHashMap<String, PluginRequest> plugins, String vcluster) {
        for (var plugin : plugins.entrySet()) {
            var pluginName = plugin.getKey();
            log.info("Configuring " + pluginName);
            var pluginBody = plugin.getValue();
            given()
                    .baseUri("http://localhost:8888/admin/interceptors/v1")
                    .auth()
                    .basic(ADMIN_USER, ADMIN_PASSWORD)
                    .body(pluginBody)
                    .contentType(ContentType.JSON)
                    .when()
                    .post("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, pluginName)
                    .then()
                    .statusCode(200)
                    .extract()
                    .response();
        }
    }

    private static void removePlugin(String vcluster, String name) {
        given()
                .baseUri("http://localhost:8888/admin/interceptors/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON)
                .when()
                .delete("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, name)
                .then()
                .statusCode(200);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class TenantInterceptorsResponse {
        List<PluginResponse> interceptors;
    }

    private static TenantInterceptorsResponse getPlugins(String vcluster) {
        return given()
                .baseUri("http://localhost:8888/admin/interceptors/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON)
                .when()
                .get("/vcluster/{vcluster}/interceptors", vcluster)
                .then()
                .statusCode(200)
                .extract()
                .response()
                .as(TenantInterceptorsResponse.class);

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
