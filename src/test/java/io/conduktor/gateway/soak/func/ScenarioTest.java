package io.conduktor.gateway.soak.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.PluginResponse;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.Scenario.DescribeTopicsAction.DescribeTopicsActionAssertions;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
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
        var clusters = scenario.getVirtualClusters();
        clusters.put("kafka", kafka.toProperties());
        clusters.put("gateway", gateway.toProperties());
        var plugins = scenario.getPlugins();
        var actions = scenario.getActions();

        startContainer(kafka, gateway);

        if (Objects.nonNull(plugins)) {
            configurePlugins(plugins);
        }
        try (var clientFactory = new ClientFactory()) {
            int id = 0;
            for (var _action : actions) {
                step(clusters, clientFactory, ++id, _action);
            }
        }
    }

    private void step(Map<String, Properties> clusters, ClientFactory clientFactory, int id, Scenario.Action _action) throws InterruptedException, ExecutionException {
        log.info("[" + id + "] Executing " + _action.simpleMessage());

        switch (_action.getType()) {
            case STEP -> {
                var action = ((Scenario.StepAction) _action);
            }
            case DOCUMENTATION -> {
                var action = ((Scenario.DocumentationAction) _action);
            }
            case MARKDOWN -> {
                var action = ((Scenario.MarkdownAction) _action);
                log.info(action.getMarkdown());
            }
            case CREATE_VIRTUAL_CLUSTERS -> {
                var action = ((Scenario.CreateVirtualClustersAction) _action);
                List<String> names = action.getNames();
                String username = "sa";
                for (String name : names) {
                    VClusterCreateResponse response = createVirtualCluster(name, username);

                    Properties properties = clusters.getOrDefault(name, new Properties());
                    properties.put("bootstrap.servers", "localhost:6969");
                    properties.put("security.protocol", "SASL_PLAINTEXT");
                    properties.put("sasl.mechanism", "PLAIN");
                    properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + response.getToken() + "\";");
                    clusters.put(name, properties);
                }
            }
            case CREATE_TOPICS -> {
                var action = ((Scenario.CreateTopicsAction) _action);
                Properties properties = getProperties(clusters, action);
                try (var kafkaAdminClient = clientFactory.kafkaAdmin(properties)) {
                    for (Scenario.CreateTopicsAction.CreateTopicRequest topic : action.getTopics()) {
                        createTopic(kafkaAdminClient,
                                topic.getName(),
                                topic.getPartitions(),
                                topic.getReplicationFactor());
                    }
                }
            }
            case LIST_TOPICS -> {
                var action = ((Scenario.ListTopicsAction) _action);
                try (var kafkaAdminClient = clientFactory.kafkaAdmin(getProperties(clusters, action))) {
                    Set<String> topics = kafkaAdminClient.listTopics().names().get();
                    if (Objects.nonNull(action.assertSize)) {
                        assertThat(topics)
                                .hasSize(action.assertSize);
                    }
                    assertThat(topics)
                            .containsAll(action.getAssertExists());
                    assertThat(topics)
                            .doesNotContainAnyElementsOf(action.getAssertDoesNotExist());
                }
            }
            case DESCRIBE_TOPICS -> {
                var action = ((Scenario.DescribeTopicsAction) _action);
                try (var kafkaAdminClient = clientFactory.kafkaAdmin(getProperties(clusters, action))) {
                    Map<String, TopicDescription> topics = kafkaAdminClient
                            .describeTopics(action.topics)
                            .allTopicNames()
                            .get();
                    for (DescribeTopicsActionAssertions assertion : action.getAssertions()) {
                        assertThat(topics)
                                .containsKey(assertion.getName());
                        TopicDescription topicDescription = topics.get(assertion.getName());
                        assertThat(topicDescription.partitions())
                                .hasSize(assertion.getPartitions());
                        assertThat(topicDescription.partitions().get(0).replicas())
                                .hasSize(assertion.getReplicationFactor());
                    }
                }
            }
            case PRODUCE -> {
                var action = ((Scenario.ProduceAction) _action);
                try (var kafkaProducer = clientFactory.kafkaProducer(getProperties(clusters, action))) {
                    produce(action.getTopic(), action.getMessages(), kafkaProducer);
                }
            }
            case CONSUME -> {
                var action = ((Scenario.ConsumeAction) _action);
                try (var consumer = clientFactory.consumer(getProperties(clusters, action))) {
                    var records = KafkaActionUtils.consume(consumer, action.getTopics(), action.getMaxMessages(), action.getTimeout());
                    if (Objects.nonNull(action.getAssertSize())) {
                        assertThat(records.size())
                                .isGreaterThanOrEqualTo(action.getAssertSize());
                    }
                    assertRecords(records, action.getAssertions());
                }
            }
            case ADD_INTERCEPTORS -> {
                var action = ((Scenario.AddInterceptorAction) _action);
                configurePlugins(action.getInterceptors());
            }
            case REMOVE_INTERCEPTORS -> {
                var action = ((Scenario.RemoveInterceptorAction) _action);
                for (String name : action.getNames()) {
                    removePlugin(action.vcluster, name);
                }
            }
            case LIST_INTERCEPTORS -> {
                var action = ((Scenario.ListInterceptorAction) _action);
                TenantInterceptorsResponse response = getPlugins(action.vcluster);

                if (Objects.nonNull(action.assertSize)) {
                    assertThat(response.interceptors)
                            .hasSize(action.assertSize);
                }
                for (String assertion : action.getAssertNames()) {
                    assertThat(response.interceptors)
                            .extracting(PluginResponse::getName)
                            .contains(assertion);
                }
            }
            case BASH -> {
                var action = ((Scenario.BashAction) _action);
                log.info("Executing " + action.getScript());
                execute("bash", action);
            }
            case SH -> {
                var action = ((Scenario.ShAction) _action);
                log.info("Executing " + action.getScript());
                execute("sh", action);
            }
            case DESCRIBE_KAFKA_PROPERTIES -> {
                var action = ((Scenario.DescribeKafkaPropertiesAction) _action);
                Properties properties = clusters.get(action.getKafka());
                assertThat(properties)
                        .isNotNull();
                assertThat(properties.keySet())
                        .containsAll(action.assertKeys);
                assertThat(properties.values())
                        .containsAll(action.assertValues);
            }
        }
    }

    private void execute(String script, Scenario.ScriptAction action) {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("bash_script", ".sh");
            FileUtils.writeStringToFile(tempFile, action.getScript(), Charset.defaultCharset());

            Process process = Runtime.getRuntime().exec(new String[]{script, tempFile.getAbsolutePath()});

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String ret = "";
                String line;
                while ((line = reader.readLine()) != null) {
                    ret = ret + line + "\n";
                }
                int exitCode = process.waitFor();
                if (action.assertExitCode != null) {
                    assertThat(exitCode)
                            .isEqualTo(action.assertExitCode);
                }
                assertThat(ret)
                        .containsSequence(action.assertOutputContains)
                        .doesNotContain(action.assertOutputDoesNotContain);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
    }

    private Properties getProperties(Map<String, Properties> virtualClusters, Scenario.KafkaAction action) {
        if (StringUtils.isBlank(action.getKafka())) {
            throw new RuntimeException("kafka is required for " + action.getType());
        }
        Properties p = new Properties();
        if (action.getProperties() != null) {
            p.putAll(action.getProperties());
        }
        Properties t = virtualClusters.get(action.getKafka());
        if (t != null) {
            p.putAll(t);
        }
        return p;
    }

    private static void createTopic(AdminClient kafkaAdminClient, String topic, int partitions, int replicationFactor) throws InterruptedException {
        KafkaActionUtils.createTopic(kafkaAdminClient, topic, partitions, (short) replicationFactor, Map.of(), 10);
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VClusterCreateRequest {
        public long lifeTimeSeconds = 7776000;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VClusterCreateResponse {
        public String token;
    }

    private static VClusterCreateResponse createVirtualCluster(String vcluster, String username) {
        log.info("Creating virtual cluster " + vcluster);
        return given()
                .baseUri("http://localhost:8888/admin/vclusters/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON)
                .body(new VClusterCreateRequest()).
                when()
                .post("/vcluster/{vcluster}/username/{username}", vcluster, username).
                then()
                .statusCode(200)
                .extract()
                .response()
                .as(VClusterCreateResponse.class);
    }

    private static void removePlugin(String vcluster, String name) {
        given()
                .baseUri("http://localhost:8888/admin/interceptors/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON).
                when()
                .delete("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, name).
                then()
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
                .contentType(ContentType.JSON).
                when()
                .get("/vcluster/{vcluster}/interceptors", vcluster).
                then()
                .statusCode(200)
                .extract()
                .response()
                .as(TenantInterceptorsResponse.class);

    }

    private static void assertRecords(List<ConsumerRecord<String, String>> records, List<Scenario.RecordAssertion> recordAssertions) {
        for (Scenario.RecordAssertion recordAssertion : recordAssertions) {
            assertRecords(records, recordAssertion);
        }
    }

    private static void assertRecords(List<ConsumerRecord<String, String>> records, Scenario.RecordAssertion recordAssertion) {
        if (StringUtils.isNotBlank(recordAssertion.getDescription())) {
            log.info("Test: " + recordAssertion.getDescription());
        }
        for (ConsumerRecord<String, String> record : records) {
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
    }

    private static void assertData(String data, Scenario.Assertion assertion) {
        if (Objects.isNull(assertion)) {
            return;
        }
        var expected = assertion.getExpected();
        var assertMethod = assertion.getOperator();
        if (StringUtils.isBlank(expected)) {
            return;
        }
        try {

            switch (assertMethod) {
                case "satisfied" -> {
                    try {
                        var dataAsMap = MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {
                        });
                        var parser = new SpelExpressionParser();
                        var context = new StandardEvaluationContext(dataAsMap);
                        context.setVariables(dataAsMap);

                        var satisfied = parser.parseExpression(String.valueOf(expected)).getValue(context, Boolean.class);
                        assertEquals(Boolean.TRUE, satisfied);
                    } catch (Exception e) {

                    }
                }
                case "isBlank" -> {
                    assertThat(data)
                            .isBlank();
                }
                case "isNotBlank" -> {
                    assertThat(data)
                            .isNotBlank();
                }
                case "containsIgnoreCase" -> {
                    assertThat(data)
                            .containsIgnoringCase(expected);
                }
                case "contains" -> {
                    assertThat(data)
                            .contains(expected);
                }
                case "doesNotContain" -> {
                    assertThat(data)
                            .doesNotContain(expected);
                }
                case "doesNotContainIgnoringCase" -> {
                    assertThat(data)
                            .doesNotContainIgnoringCase(expected);
                }
            }
        } catch (Throwable ex) {
            fail(ExceptionUtils.getRootCause(ex));
        }
    }


}
