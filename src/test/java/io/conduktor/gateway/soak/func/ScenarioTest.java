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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.DockerComposeUtils.getUpdatedDockerCompose;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    private static List<Arguments> scenarios;

    public static Path createRandomFolder(Boolean deleteOnExit) {
        try {
            Path path = Paths.get(System.getProperty("user.dir"), UUID.randomUUID().toString());
            Files.createDirectory(path);
            if (deleteOnExit) {
                path.toFile().deleteOnExit();
            }
            return path;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean deleteFolderOnExist = false;
    private static boolean cleanupAfterTest = false;

    private static Path executionFolder = createRandomFolder(deleteFolderOnExist);

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
                    if (isScenario(file)) {
                        scenarios.add(Arguments.of(configReader.readYamlInResources(file.getPath())));
                    }
                }
            }
        } else {
            log.error("The specified folder does not exist or is not a directory.");
        }
    }

    private static boolean isScenario(File file) {
        return file.isFile() && file.getName().toLowerCase().endsWith(".yaml");
    }

    private static Stream<Arguments> sourceForScenario() {
        return scenarios.stream();
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(Scenario scenario) throws Exception {
        log.info("Start to test: {}", scenario.getTitle());
        var actions = scenario.getActions();

        Map<String, Properties> clusters = scenario.toServiceProperties();
        var composeFileContent = getUpdatedDockerCompose(scenario);
        System.out.println(executionFolder);
        FileUtils.writeStringToFile(new File(executionFolder.getFileName() + "/docker-compose.yaml"), composeFileContent, Charset.defaultCharset());
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.directory(executionFolder.toFile());
        processBuilder.command("docker", "compose", "up", "--wait", "--detach");
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String ret = "";
            String line;
            while ((line = reader.readLine()) != null) {
                ret = ret + line + "\n";
                System.out.println(line);
            }
        }
        process.waitFor();

        try (var clientFactory = new ClientFactory()) {
            int id = 0;
            for (var _action : actions) {
                step(clusters, clientFactory, ++id, _action);
            }
        }
    }

    @AfterAll
    public static void cleanup() throws IOException, InterruptedException {
        if (cleanupAfterTest) {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.directory(executionFolder.toFile());
            processBuilder.command("docker", "compose", "down", "--volume");
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String ret = "";
                String line;
                while ((line = reader.readLine()) != null) {
                    ret = ret + line + "\n";
                    System.out.println(line);
                }
            }
            process.waitFor();
        }
    }

    private void step(Map<String, Properties> clusters, ClientFactory clientFactory, int id, Scenario.Action _action) throws Exception {
        log.info("[" + id + "] Executing " + _action.simpleMessage());

        switch (_action.getType()) {
            case STEP -> {
                var action = ((Scenario.StepAction) _action);
            }
            case DOCUMENTATION -> {
                var action = ((Scenario.DocumentationAction) _action);
            }
            case SUCCESS -> {
                var action = ((Scenario.SuccessAction) _action);
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

                    File propertiesFile = new File(executionFolder + "/" + name + ".properties");
                    savePropertiesToFile(propertiesFile, properties);

                }
            }
            case CREATE_TOPICS -> {
                var action = ((Scenario.CreateTopicsAction) _action);
                try (var adminClient = clientFactory.kafkaAdmin(getProperties(clusters, action))) {
                    for (Scenario.CreateTopicsAction.CreateTopicRequest topic : action.getTopics()) {
                        try {
                            createTopic(adminClient,
                                    topic.getName(),
                                    topic.getPartitions(),
                                    topic.getReplicationFactor());
                            if (action.getAssertError() != null) {
                                Assertions.fail("Expected an error");
                            }
                        } catch (Exception e) {
                            if (!action.getAssertErrorMessages().isEmpty()) {
                                assertThat(e.getMessage())
                                        .containsIgnoringWhitespaces(action.getAssertErrorMessages().toArray(new String[0]));
                            }
                            log.warn(topic + " creation failed", e);
                        }
                    }
                }
            }
            case LIST_TOPICS -> {
                var action = ((Scenario.ListTopicsAction) _action);
                try (var adminClient = clientFactory.kafkaAdmin(getProperties(clusters, action))) {
                    Set<String> topics = adminClient.listTopics().names().get();
                    System.out.println(topics);
                    if (Objects.nonNull(action.assertSize)) {
                        assertThat(topics)
                                .hasSize(action.assertSize);
                    }
                    if (!action.getAssertExists().isEmpty()) {
                        assertThat(topics)
                                .containsAll(action.getAssertExists());
                    }
                    if (!action.getAssertDoesNotExist().isEmpty()) {
                        assertThat(topics)
                                .doesNotContainAnyElementsOf(action.getAssertDoesNotExist());
                    }
                }
            }
            case DESCRIBE_TOPICS -> {
                var action = ((Scenario.DescribeTopicsAction) _action);
                try (var adminClient = clientFactory.kafkaAdmin(getProperties(clusters, action))) {
                    Map<String, TopicDescription> topics = adminClient
                            .describeTopics(action.topics)
                            .allTopicNames()
                            .get();
                    for (DescribeTopicsActionAssertions assertion : action.getAssertions()) {
                        assertThat(topics)
                                .containsKey(assertion.getName());
                        TopicDescription topicDescription = topics.get(assertion.getName());
                        if (assertion.getPartitions() != null) {
                            assertThat(topicDescription.partitions())
                                    .hasSize(assertion.getPartitions());
                        }
                        if (assertion.getReplicationFactor() != null) {
                            assertThat(topicDescription.partitions().get(0).replicas())
                                    .hasSize(assertion.getReplicationFactor());
                        }
                    }
                }
            }
            case PRODUCE -> {
                var action = ((Scenario.ProduceAction) _action);
                Properties properties = getProperties(clusters, action);
                try (var producer = clientFactory.kafkaProducer(properties)) {
                    try {
                        produce(action.getTopic(), action.getMessages(), producer);
                        if (action.getAssertError() != null) {
                            Assertions.fail("Produce should have failed");
                        }
                    } catch (Exception e) {
                        if (!action.getAssertErrorMessages().isEmpty()) {
                            assertThat(e.getMessage())
                                    .contains(action.getAssertErrorMessages());
                        }
                        log.error("could not produce message");
                    }
                }
            }
            case CONSUME -> {
                var action = ((Scenario.ConsumeAction) _action);
                try (var consumer = clientFactory.consumer(getProperties(clusters, action))) {
                    var records = KafkaActionUtils.consume(consumer, action.getTopics(), action.getMaxMessages(), action.getTimeout());
                    if (action.isShowRecords()) {
                        records.stream().forEach(System.out::println);
                    }
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
                execute(id, "bash", action, getProperties(clusters, action));
            }
            case SH -> {
                var action = ((Scenario.ShAction) _action);
                execute(id, "sh", action, getProperties(clusters, action));
            }
            case DESCRIBE_KAFKA_PROPERTIES -> {
                var action = ((Scenario.DescribeKafkaPropertiesAction) _action);
                Properties properties = clusters.get(action.getKafka());
                assertThat(properties)
                        .isNotNull();
                if (!action.assertKeys.isEmpty()) {
                    assertThat(properties.keySet())
                            .containsAll(action.assertKeys);
                }
                if (!action.assertValues.isEmpty()) {
                    assertThat(properties.values())
                            .containsAll(action.assertValues);
                }
            }
        }
    }

    private void savePropertiesToFile(File propertiesFile, Properties properties) throws IOException {
        String content = properties.keySet().stream().map(key -> key + "=" + properties.get(key)).collect(Collectors.joining("\n"));
        FileUtils.writeStringToFile(propertiesFile, content, Charset.defaultCharset());
    }

    private void execute(int id, String script, Scenario.ScriptAction action, Properties properties) {
        File scriptFile = null;
        try {
            scriptFile = new File(executionFolder + "/step-" + id + "." + script);
            String data = (action.getScript().startsWith("#!/bin") ? "" : "#!/bin/" + script + "\n") + action.getScript();
            FileUtils.writeStringToFile(scriptFile, data, Charset.defaultCharset());

            Map<String, String> map = new HashMap<>(properties.size());
            for (String key : properties.stringPropertyNames()) {
                String formattedKey = key.toUpperCase().replace(".", "_");
                String value = properties.getProperty(key);
                map.put(formattedKey, value);
            }

            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.directory(executionFolder.toFile());
            processBuilder.environment().putAll(map);
            processBuilder.command(script, scriptFile.getAbsolutePath());
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();


            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String ret = "";
                String line;
                while ((line = reader.readLine()) != null) {
                    ret = ret + line + "\n";
                }
                int exitCode = process.waitFor();

                System.out.println(exitCode);
                if (action.showOutput) {
                    log.info(ret);
                }
                if (action.assertExitCode != null) {
                    assertThat(exitCode)
                            .isEqualTo(action.assertExitCode);
                }
                if (!action.assertOutputContains.isEmpty()) {
                    assertThat(ret)
                            .containsSequence(action.assertOutputContains);
                }
                if (!action.assertOutputDoesNotContain.isEmpty()) {
                    assertThat(ret)
                            .doesNotContain(action.assertOutputDoesNotContain);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (scriptFile != null) {
//                scriptFile.delete();
            }
        }
    }

    private Properties getProperties(Map<String, Properties> virtualClusters, Scenario.KafkaAction action) {
        if (StringUtils.isBlank(action.getKafka())) {
            throw new RuntimeException("kafka is required for " + action.getType());
        }
        Properties p = new Properties();
        Properties t = virtualClusters.get(action.getKafka());
        if (t == null) {
            throw new RuntimeException("No kafka defined for " + action.getKafka());
        }
        if (t != null) {
            p.putAll(t);
        }
        if (action.getProperties() != null) {
            p.putAll(action.getProperties());
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

        List<String> keys = records.stream().map(ConsumerRecord::key).toList();
        List<String> values = records.stream().map(ConsumerRecord::value).toList();
        List<Header> headers = records.stream().flatMap(r -> getHeaders(r).stream()).toList();


        for (Scenario.RecordAssertion recordAssertion : recordAssertions) {
            boolean validKey = validate(recordAssertion.getKey(), keys);
            boolean validValues = validate(recordAssertion.getValue(), values);
            boolean validHeader = validateHeaders(recordAssertion, headers);
            if (StringUtils.isNotBlank(recordAssertion.getDescription())) {
                log.info("Test: " + recordAssertion.getDescription());
            }
            if ((validKey && validValues && validHeader) == false) {
                log.info("Assertion failed");
                log.info("Key: " + validKey);
                log.info("Values: " + validValues);
                log.info("Header: " + validHeader);
                Assertions.fail(recordAssertion.getDescription() + " failed");
            }
        }
    }

    private static boolean validateHeaders(Scenario.RecordAssertion recordAssertion, List<Header> headers) {
        if (recordAssertion.getHeaders() == null) {
            return true;
        }
        for (String headerKey : recordAssertion.getHeaders().keySet()) {
            Scenario.Assertion headerAssertion = recordAssertion.getHeaders().get(headerKey);
            if (headerAssertion == null) {
                return false;
            }
            List<String> headerValues = headers.stream().filter(e -> headerKey.equals(e.key())).map(h -> new String(h.value())).toList();
            if (validate(headerAssertion, headerValues)) {
                return false;
            }
        }
        return true;
    }

    public static boolean validate(Scenario.Assertion assertion, List<String> data) {
        if (assertion == null) {
            return true;
        }
        return data.stream().filter(value -> validate(assertion, value)).findFirst().isPresent();
    }

    public static boolean validate(Scenario.Assertion assertion, String data) {
        String expected = assertion.getExpected();
        return switch (assertion.getOperator()) {
            case "satisfies" -> satisfies(data, expected);
            case "isBlank" -> StringUtils.isBlank(data);
            case "isNotBlank" -> StringUtils.isNotBlank(data);
            case "containsIgnoreCase" -> StringUtils.containsIgnoreCase(data, expected);
            case "contains" -> StringUtils.contains(data, expected);
            case "doesNotContain" -> !StringUtils.contains(data, expected);
            case "doesNotContainIgnoringCase" -> !StringUtils.containsIgnoreCase(data, expected);
            default -> throw new RuntimeException(assertion.getOperator() + " is not supported");
        };
    }

    private static List<Header> getHeaders(ConsumerRecord<String, String> r) {
        return r.headers() == null ? List.of() : List.of(r.headers().toArray());
    }

    private static boolean satisfies(String data, String expected) {
        try {
            var dataAsMap = MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {
            });
            var parser = new SpelExpressionParser();
            var context = new StandardEvaluationContext(dataAsMap);
            context.setVariables(dataAsMap);

            return parser.parseExpression(String.valueOf(expected)).getValue(context, Boolean.class);

        } catch (Exception e) {
            return false;
        }
    }
}
