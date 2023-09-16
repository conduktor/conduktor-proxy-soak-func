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
import org.jetbrains.annotations.NotNull;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.DockerComposeUtils.getUpdatedDockerCompose;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectory;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.lang3.StringUtils.trimToEmpty;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";

    private static boolean cleanupAfterTest = false;

    private static File executionFolder;
    private static File scenarioFolder;


    @BeforeAll
    public void setUp() throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "docker rm -f $(docker ps -aq)");
        processBuilder.start().waitFor();

        executionFolder = createDirectory(Paths.get("target", UUID.randomUUID().toString())).toFile();
    }

    private static boolean isScenario(File file) {
        return file.isFile() && "scenario.yaml".equals(file.getName());
    }

    private static Stream<Arguments> sourceForScenario() throws Exception {
        var rootFolder = new File(SCENARIO_DIRECTORY_PATH);
        if (!rootFolder.exists() || !rootFolder.isDirectory()) {
            log.error("The specified rootFolder does not exist or is not a directory.");
            return Stream.empty();
        }
        var scenerioFolders = rootFolder.listFiles(File::isDirectory);
        if (scenerioFolders == null) {
            log.error("No sub scenerioFolders in " + rootFolder);
            return Stream.empty();
        }

        var scenarioYamlConfigReader = YamlConfigReader.forType(Scenario.class);
        List<Arguments> scenarios = new ArrayList<>();
        for (var scenarioFolder : scenerioFolders) {
            File executionFolder = new File(ScenarioTest.executionFolder.getPath() + "/" + scenarioFolder.getName());
            FileUtils.copyDirectory(scenarioFolder, executionFolder);
            for (var file : scenarioFolder.listFiles()) {
                if (isScenario(file)) {
                    Scenario scenario = scenarioYamlConfigReader.readYamlInResources(file.getPath());
                    scenarios.add(Arguments.of(executionFolder, scenario));
                }
            }
        }
        return Stream.of(scenarios.toArray(new Arguments[0]));
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(File folder, Scenario scenario) throws Exception {
        scenarioFolder = folder;
        log.info("Execution folder {}", scenarioFolder.getAbsolutePath());


        log.info("Start to test: {}", scenario.getTitle());
        var actions = scenario.getActions();
        createDirectory(Path.of(scenarioFolder.getAbsolutePath(), "/asciinema"));
        createDirectory(Path.of(scenarioFolder.getAbsolutePath(), "/images"));

        appendTo(
                "docker-compose.yaml",
                getUpdatedDockerCompose(scenario));
        appendTo("run.sh", "");
        appendTo("type.sh", """
                echo 'function execute() {
                    file=$1
                    chars=$(cat $file| wc -c)
                    if [ "$chars" -lt 100 ] ; then
                        cat $file | pv -qL 50
                    elif [ "$chars" -lt 250 ] ; then
                        cat $file | pv -qL 100
                    elif [ "$chars" -lt 500 ] ; then
                        cat $file | pv -qL 200
                    else
                        cat $file | pv -qL 400
                    fi
                    . $file
                }
                                
                execute $1
                """);
        new File(scenarioFolder.getAbsolutePath() + "/type.sh").setExecutable(true);
        new File(scenarioFolder.getAbsolutePath() + "/run.sh").setExecutable(true);
        runScenarioSteps(scenario, actions);
    }

    private Scenario scenario;

    private void runScenarioSteps(Scenario scenario, LinkedList<Scenario.Action> actions) throws Exception {
        Map<String, Properties> clusters = scenario.toServiceProperties();
        this.scenario = scenario;
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
            processBuilder.directory(scenarioFolder);
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

    private void step(Map<String, Properties> clusters, ClientFactory clientFactory, int _id, Scenario.Action _action) throws Exception {
        String id = format("%02d", _id);
        log.info("[" + id + "] Executing " + _action.simpleMessage());

        appendTo("Readme.md",
                format("""
                                %s
                                %s
                                                                
                                """,
                        _action.markdownHeader(),
                        trimToEmpty(_action.getMarkdown())));

        switch (_action.getType()) {
            case INTRODUCTION, CONCLUSION, STEP -> {
                //
            }
            case FILE -> {
                var action = ((Scenario.FileAction) _action);
                appendTo("/Readme.md",
                        format("""
                                        ```sh
                                        cat %s
                                        ```
                                                                
                                        <details>
                                          <summary>Results</summary>
                                          
                                        ```%s
                                        %s
                                        ```
                                          
                                        </details>
                                                            
                                        """,
                                action.filename,
                                getExtension(action.filename),
                                trimToEmpty(readFileToString(new File(scenarioFolder.getAbsoluteFile() + "/" + action.filename), Charset.defaultCharset())
                                )));
            }
            case CREATE_VIRTUAL_CLUSTERS -> {
                var action = ((Scenario.CreateVirtualClustersAction) _action);
                LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                String gateway = gatewayProperties.get("gateway.host");
                VClusterCreateResponse response = createVirtualCluster(gateway, action.getName(), action.getServiceAccount());

                Properties properties = clusters.getOrDefault(action.getName(), new Properties());
                properties.put("bootstrap.servers", gatewayProperties.get("bootstrap.servers"));
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + action.getServiceAccount() + "' password='" + response.getToken() + "';");
                clusters.put(action.getName(), properties);

                savePropertiesToFile(new File(scenarioFolder + "/" + action.getName() + "-" + action.getServiceAccount() + ".properties"), properties);

                code(scenario, action, id, """
                                token=$(curl \\
                                    --silent \\
                                    --request POST "%s/admin/vclusters/v1/vcluster/%s/username/%s" \\
                                    --user 'admin:conduktor' \\
                                    --header 'Content-Type: application/json' \\
                                    --data-raw '{"lifeTimeSeconds": 7776000}' | jq -r ".token")
                                    
                                echo  \"\"\"
                                bootstrap.servers=%s
                                security.protocol=SASL_PLAINTEXT
                                sasl.mechanism=PLAIN
                                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='$token';
                                \"\"\" > %s-%s.properties
                                                                
                                cat %s-%s.properties
                                """,
                        gateway,
                        action.getName(),
                        action.getServiceAccount(),
                        gatewayProperties.get("bootstrap.servers"),
                        action.getServiceAccount(),
                        action.getName(),
                        action.getServiceAccount(),
                        action.getName(),
                        action.getServiceAccount());

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

                            code(scenario, action, id + "-" + topic.getName(), """
                                            kafka-topics \\
                                                --bootstrap-server %s \\
                                                --command-config %s \\
                                                --replication-factor %s \\
                                                --partitions %s \\
                                                --create --if-not-exists \\
                                                --topic %s
                                            """,
                                    clusters.get(action.getKafka()).getProperty("bootstrap.servers"),
                                    action.getKafkaConfig(),
                                    "" + topic.getReplicationFactor(),
                                    "" + topic.getPartitions(),
                                    topic.getName()

                            );
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
                    code(scenario, action, id, """
                                    kafka-topics \\
                                        --bootstrap-server %s%s \\
                                        --list
                                    """,
                            clusters.get(action.getKafka()).getProperty("bootstrap.servers"),
                            action.getKafkaConfig() == null ? "" : " \\\n    --command-config " + action.getKafkaConfig());
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
                    for (String topic : action.topics) {
                        code(scenario, action, id, """
                                        kafka-topics \\
                                            --bootstrap-server %s%s \\
                                            --describe \\
                                            --topic %s
                                        """,
                                clusters.get(action.getKafka()).getProperty("bootstrap.servers"),
                                action.getKafkaConfig() == null ? "" : " \\\n    --command-config " + action.getKafkaConfig(),
                                topic);
                    }
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


                        String command = action.getMessages()
                                .stream()
                                .map(message ->
                                        format("""
                                                        echo '%s' | \\
                                                            kafka-console-producer \\
                                                                --bootstrap-server %s%s \\
                                                                --topic %s
                                                        """,
                                                message.getValue(),
                                                clusters.get(action.getKafka()).getProperty("bootstrap.servers"),
                                                action.getKafkaConfig() == null ? "" : " \\\n        --producer.config " + action.getKafkaConfig(),
                                                action.getTopic()))
                                .collect(Collectors.joining("\n"));
                        code(scenario, action, id, StringUtils.removeEnd(command, "\n"));


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
                Properties properties = getProperties(clusters, action);
                if (!properties.containsKey("group.id")) {
                    properties.put("group.id", "step-" + id);
                }
                if (!properties.containsKey("auto.offset.reset")) {
                    properties.put("auto.offset.reset", "earliest");
                }
                final long timeout;
                if (action.getTimeout() != null) {
                    timeout = action.getTimeout();
                } else if (action.getAssertSize() != null || action.getMaxMessages() == null) {
                    timeout = TimeUnit.SECONDS.toMillis(5);
                } else {
                    timeout = TimeUnit.MINUTES.toMillis(1);
                }

                int maxRecords;
                if (action.getMaxMessages() != null) {
                    maxRecords = action.getMaxMessages();
                } else if (action.getAssertSize() != null) {
                    maxRecords = action.getAssertSize() + 1;
                } else {
                    maxRecords = 100;
                }

                try (var consumer = clientFactory.consumer(properties)) {
                    consumer.subscribe(Arrays.asList(action.getTopic()));
                    int recordCount = 0;
                    long startTime = System.currentTimeMillis();
                    var records = new ArrayList<ConsumerRecord<String, String>>();

                    while (recordCount < maxRecords || (action.getAssertSize() != null && recordCount > action.getAssertSize())) {
                        if (!(System.currentTimeMillis() < startTime + timeout)) break;
                        var consumedRecords = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                        recordCount += consumedRecords.count();
                        for (var record : consumedRecords) {
                            if (action.isShowRecords()) {
                                System.out.println("[p:" + record.partition() + "/o:" + record.offset() + "] " + record.value());
                            }
                            records.add(record);
                        }
                    }
                    assertThat(records).isNotNull();
                    if (Objects.nonNull(action.getAssertSize())) {
                        assertThat(records.size())
                                .isGreaterThanOrEqualTo(action.getAssertSize());
                    }
                    assertRecords(records, action.getAssertions());
                }
                code(scenario, action, id, """
                                kafka-console-consumer \\
                                    --bootstrap-server %s%s \\
                                    --group %s \\
                                    --topic %s%s%s%s
                                """,
                        clusters.get(action.getKafka()).getProperty("bootstrap.servers"),
                        action.getKafkaConfig() == null ? "" : " \\\n    --consumer.config " + action.getKafkaConfig(),
                        properties.getProperty("group.id"),
                        action.getTopic(),
                        "earliest".equals(properties.get("auto.offset.reset")) ? " \\\n    --from-beginning" : "",
                        action.getMaxMessages() == null ? "" : " \\\n    --max-messages " + maxRecords,
                        action.getAssertSize() == null ? "" : " \\\n    --timeout-ms  " + timeout
                );
            }
            case FAILOVER -> {
                var action = ((Scenario.FailoverAction) _action);
                LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                String gateway = gatewayProperties.get("gateway.host");
                given()
                        .baseUri(gateway + "/admin/pclusters/v1")
                        .auth()
                        .basic(ADMIN_USER, ADMIN_PASSWORD)
                        .contentType(ContentType.JSON)
                        .when()
                        .post("/pcluster/{from}/switch?to={to}", action.from, action.to)
                        .then()
                        .statusCode(SC_OK)
                        .extract()
                        .response();

                code(scenario, action, id, """
                                curl \\
                                  --silent \\
                                  --user "admin:conduktor" \\
                                  --request POST '%s/admin/pclusters/v1/pcluster/%s/switch?to=%s'
                                """,
                        gateway,
                        action.from,
                        action.to);
            }
            case ADD_INTERCEPTORS -> {
                var action = ((Scenario.AddInterceptorAction) _action);
                LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                String gateway = gatewayProperties.get("gateway.host");

                configurePlugins(gateway, action.getInterceptors());
                LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors = action.getInterceptors();
                for (var request : interceptors.entrySet()) {
                    LinkedHashMap<String, PluginRequest> plugins = request.getValue();
                    String vcluster = request.getKey();
                    for (var plugin : plugins.entrySet()) {
                        var pluginName = plugin.getKey();
                        var pluginBody = plugin.getValue();

                        code(scenario, action, id, """
                                        curl \\
                                            --silent \\
                                            --request POST "%s/admin/vclusters/v1/vcluster/%s/interceptor/%s" \\
                                            --user 'admin:conduktor' \\
                                            --header 'Content-Type: application/json' \\
                                            --data-raw '%s'
                                        """,
                                gateway,
                                vcluster,
                                pluginName,
                                new ObjectMapper().writeValueAsString(pluginBody));
                    }
                }
            }
            case REMOVE_INTERCEPTORS -> {
                var action = ((Scenario.RemoveInterceptorAction) _action);
                LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                String gateway = gatewayProperties.get("gateway.host");
                for (String name : action.getNames()) {
                    removePlugin(gateway, action.vcluster, name);
                    code(scenario, action, id, """
                                    curl \\
                                        --silent \\
                                        --request POST "%s/admin/vclusters/v1/vcluster/%s/interceptor/%s" \\
                                        --user 'admin:conduktor' \\
                                        --header 'Content-Type: application/json' \\
                                        --data-raw '%s'
                                    """,
                            gateway,
                            name);
                }
            }
            case LIST_INTERCEPTORS -> {
                var action = ((Scenario.ListInterceptorAction) _action);
                LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                String gateway = gatewayProperties.get("gateway.host");
                TenantInterceptorsResponse response = getPlugins(gateway, action.vcluster);

                if (Objects.nonNull(action.assertSize)) {
                    assertThat(response.interceptors)
                            .hasSize(action.assertSize);
                }
                for (String assertion : action.getAssertNames()) {
                    assertThat(response.interceptors)
                            .extracting(PluginResponse::getName)
                            .contains(assertion);
                }
                code(scenario, action, id, """
                                curl \\
                                    -u "admin:conduktor" \\
                                    --request GET "%s/admin/interceptors/v1/vcluster/%s/interceptors" \\
                                    --header 'Content-Type: application/json'| jq
                                """,
                        gateway,
                        action.vcluster);
            }
            case DOCKER -> {
                var action = ((Scenario.DockerAction) _action);

                ProcessBuilder processBuilder = new ProcessBuilder();
                processBuilder.directory(scenarioFolder);
                if (action.isDaemon()) {
                    processBuilder.command("sh", "-c", "nohup " + ((Scenario.CommandAction) action).getCommand() + "&");
                } else {
                    processBuilder.command("sh", "-c", ((Scenario.CommandAction) action).getCommand());
                }
                processBuilder.redirectErrorStream(true);
                Process process = processBuilder.start();

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String ret = "";
                    String line;
                    while ((line = reader.readLine()) != null) {
                        ret = ret + line + "\n";
                    }
                    if (!action.isDaemon()) {
                        int exitCode = process.waitFor();

                        if (action.isShowOutput()) {
                            log.info(ret);
                        }
                        if (action.getAssertExitCode() != null) {
                            assertThat(exitCode)
                                    .isEqualTo(action.getAssertExitCode());
                        }
                        if (!action.getAssertOutputContains().isEmpty()) {
                            assertThat(ret)
                                    .contains(action.getAssertOutputContains());
                        }
                        if (!action.getAssertOutputDoesNotContain().isEmpty()) {
                            assertThat(ret)
                                    .doesNotContain(action.getAssertOutputDoesNotContain());
                        }
                    }
                }
                code(scenario, action, id,
                        "%s",
                        ((Scenario.CommandAction) action).getCommand());

            }
            case SH -> {
                var action = ((Scenario.ShAction) _action);
                Properties properties = getProperties(clusters, action);
                String expandedScript = action.getScript();

                var env = new HashMap<String, String>();
                for (String key : properties.stringPropertyNames()) {
                    String formattedKey = key.toUpperCase().replace(".", "_");
                    String value = properties.getProperty(key);
                    expandedScript = StringUtils.replace(expandedScript, "${" + formattedKey + "}", value);
                    env.put(formattedKey, value);
                }

                if (action.getGateway() != null) {
                    LinkedHashMap<String, String> gatewayProperties = scenario.getServices().get(action.getGateway()).getProperties();
                    expandedScript = StringUtils.replace(expandedScript, "${GATEWAY_HOST}", gatewayProperties.get("gateway.host"));
                }

                code(scenario, action, id, StringUtils.removeEnd(expandedScript, "\n"));
                File scriptFile = new File(scenarioFolder + "/step-" + id + ".sh");
                writeStringToFile(scriptFile, addShHeader(id, action) + action.getScript(), Charset.defaultCharset());


                ProcessBuilder processBuilder = new ProcessBuilder();
                processBuilder.directory(scenarioFolder);
                processBuilder.command("sh", scriptFile.getAbsolutePath());
                processBuilder.redirectErrorStream(true);
                processBuilder.environment().putAll(env);
                Process process = processBuilder.start();


                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String ret = "";
                    String line;
                    while ((line = reader.readLine()) != null) {
                        ret = ret + line + "\n";
                    }
                    int exitCode = process.waitFor();

                    if (action.showOutput) {
                        log.info(ret);
                    }
                    if (action.assertExitCode != null) {
                        assertThat(exitCode)
                                .isEqualTo(action.assertExitCode);
                    }
                    if (!action.assertOutputContains.isEmpty()) {
                        assertThat(ret)
                                .contains(action.assertOutputContains);
                    }
                    if (!action.assertOutputDoesNotContain.isEmpty()) {
                        assertThat(ret)
                                .doesNotContain(action.assertOutputDoesNotContain);
                    }
                }
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


                code(scenario, action, id, """
                                cat clientConfig/%s
                                """,
                        action.getKafkaConfig());

            }
        }
    }

    @NotNull
    private String addShHeader(String id, Scenario.ShAction action) {
        return action.getScript().startsWith("#!/bin/sh") ? "" : "#!/bin/sh\n"
                + "echo 'Step " + id + " " + action.getType() + " " + trimToEmpty(action.getTitle()) + "'\n";
    }

    private void savePropertiesToFile(File propertiesFile, Properties properties) throws IOException {
        String content = properties.keySet().stream().map(key -> key + "=" + properties.get(key)).collect(Collectors.joining("\n"));
        writeStringToFile(propertiesFile, content, Charset.defaultCharset());
    }

    private Properties getProperties(Map<String, Properties> virtualClusters, Scenario.KafkaAction action) {
        Properties p = new Properties();
        if (StringUtils.isNotBlank(action.getKafka())) {
            Properties t = virtualClusters.get(action.getKafka());
            if (t == null) {
                throw new RuntimeException("No kafka defined for " + action.getKafka());
            }
            if (t != null) {
                p.putAll(t);
            }
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


    private static void configurePlugins(String gateway, LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> plugins) {
        for (var plugin : plugins.entrySet()) {
            configurePlugins(gateway, plugin.getValue(), plugin.getKey());
        }
    }

    private static void configurePlugins(String gateway, LinkedHashMap<String, PluginRequest> plugins, String vcluster) {
        for (var plugin : plugins.entrySet()) {
            var pluginName = plugin.getKey();
            log.info("Configuring " + pluginName);
            var pluginBody = plugin.getValue();
            given()
                    .baseUri(gateway + "/admin/interceptors/v1")
                    .auth()
                    .basic(ADMIN_USER, ADMIN_PASSWORD)
                    .body(pluginBody)
                    .contentType(ContentType.JSON)
                    .when()
                    .post("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, pluginName)
                    .then()
                    .statusCode(SC_CREATED)
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

    private static VClusterCreateResponse createVirtualCluster(String gateway, String vcluster, String username) {
        log.info("Creating virtual cluster " + vcluster);
        return given()
                .baseUri(gateway + "/admin/vclusters/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON)
                .body(new VClusterCreateRequest()).
                when()
                .post("/vcluster/{vcluster}/username/{username}", vcluster, username).
                then()
                .statusCode(SC_OK)
                .extract()
                .response()
                .as(VClusterCreateResponse.class);
    }

    private static void removePlugin(String gateway, String vcluster, String name) {
        given()
                .baseUri(gateway + "/admin/interceptors/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON).
                when()
                .delete("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, name).
                then()
                .statusCode(SC_OK);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class TenantInterceptorsResponse {
        List<PluginResponse> interceptors;
    }

    private static TenantInterceptorsResponse getPlugins(String gateway, String vcluster) {
        return given()
                .baseUri(gateway + "/admin/interceptors/v1")
                .auth()
                .basic(ADMIN_USER, ADMIN_PASSWORD)
                .contentType(ContentType.JSON).
                when()
                .get("/vcluster/{vcluster}/interceptors", vcluster).
                then()
                .statusCode(SC_OK)
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
                log.info("Assertion failed with key: " + validKey + ", values: " + validValues + ", header: " + validHeader);
                log.info("" + records);
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

    private static void code(Scenario scenario, Scenario.Action action, String id, String format, String... args) throws Exception {
        String stepTitle = "Step " + id + " " + action.getType() + ": " + trimToEmpty(action.getTitle());

        String step = "step-" + id + "-" + action.getType();
        appendTo("run.sh", "echo '" + stepTitle + "'\nsh " + step + ".sh\n\n");
        appendTo(step + ".sh", format(format, args));

        // svg-term ?
        appendTo("record.sh",
                format("""
                                echo "%s - %s"
                                asciinema rec \\
                                    --title "%s - %s" \\
                                    --idle-time-limit 2 \\
                                    --cols 140 --rows 20 \\
                                    --command "sh execute.sh %s.sh" \\
                                    asciinema/%s.asciinema
                                svg-term \\
                                    --in asciinema/%s.asciinema \\
                                    --out images/%s.svg \\
                                    --window true
                                agg \\
                                    --theme "asciinema" \\
                                    --last-frame-duration 1 \\
                                    --no-loop \\
                                    asciinema/%s.asciinema \\
                                    images/%s.gif
                                """,
                        scenario.getTitle(),
                        stepTitle,
                        scenario.getTitle(),
                        stepTitle,
                        step,
                        step,
                        step,
                        step,
                        step,
                        step));


        appendTo("/Readme.md",
                format("""
                                ```sh
                                %s
                                ```
                                                        
                                <details>
                                  <summary>Results</summary>
                                  
                                  ![%s](images/%s.svg)
                                  
                                </details>
                                                    
                                """,
                        format(format, args),
                        trimToEmpty(action.getTitle()),
                        step,
                        trimToEmpty(action.getTitle())
                ));
    }

    private static void appendTo(String filename, String code) throws IOException {
        writeStringToFile(
                new File(scenarioFolder + "/" + filename),
                code,
                Charset.defaultCharset(),
                true);
    }
}
