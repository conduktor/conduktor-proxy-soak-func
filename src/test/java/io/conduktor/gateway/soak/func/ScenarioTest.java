package io.conduktor.gateway.soak.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.conduktor.gateway.soak.func.config.PluginRequest;
import io.conduktor.gateway.soak.func.config.PluginResponse;
import io.conduktor.gateway.soak.func.config.Scenario;
import io.conduktor.gateway.soak.func.config.Scenario.AddTopicMappingAction.TopicMapping;
import io.conduktor.gateway.soak.func.config.Scenario.DescribeTopicsAction.DescribeTopicsActionAssertions;
import io.conduktor.gateway.soak.func.config.support.YamlConfigReader;
import io.conduktor.gateway.soak.func.utils.ClientFactory;
import io.conduktor.gateway.soak.func.utils.KafkaActionUtils;
import io.restassured.http.ContentType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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
import java.net.URLEncoder;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.conduktor.gateway.soak.func.utils.DockerComposeUtils.getUpdatedDockerCompose;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.http.HttpStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScenarioTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    public static final String GATEWAY_HOST = "gateway.host";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    static final String currentDate = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss").format(Calendar.getInstance().getTime());

    public static final String TYPE_SH = """
            function type_and_execute() {
              local GREEN="\\033[0;32m"
              local RESET="\\033[0m"
              local file=$1
              local chars=$(cat $file| wc -c)
                      
              printf "${GREEN}"
              if [ "$chars" -lt 70 ] ; then
                  cat $file | pv -qL 30
              elif [ "$chars" -lt 100 ] ; then
                  cat $file | pv -qL 50
              elif [ "$chars" -lt 250 ] ; then
                  cat $file | pv -qL 100
              elif [ "$chars" -lt 500 ] ; then
                  cat $file | pv -qL 200
              else
                  cat $file | pv -qL 400
              fi
              echo "${RESET}"
                      
              . $file
            }
                      
            type_and_execute $1
            """;
    public static final String RECORD_ASCIINEMA_SH = """
            for stepSh in $(ls step*sh | sort ) ; do
                echo "Processing asciinema for $stepSh"
                step=$(echo "$stepSh" | sed "s/.sh$//" )
                rows=20

                asciinema rec \\
                  --title "$step" \\
                  --idle-time-limit 2 \\
                  --cols 140 --rows $rows \\
                  --command "sh type.sh $step.sh" \\
                  asciinema/$step.asciinema

                asciinemaLines=$(asciinema play -s 1000 asciinema/$step.asciinema | wc -l)
                if [ $asciinemaLines -lt 20 ] ; then
                  rows=$asciinemaLines
                fi

                svg-term \\
                  --in asciinema/$step.asciinema \\
                  --out images/$step.svg \\
                  --height $rows \\
                  --window true

                agg \\
                  --theme "asciinema" \\
                  --last-frame-duration 5 \\
                  asciinema/$step.asciinema \\
                  images/$step.gif
            done
            """;
    public static final String RECORD_COMMAND_SH = """
            for stepSh in $(ls step*sh | sort ) ; do
                echo "Processing $stepSh"
                step=$(echo "$stepSh" | sed "s/.sh$//" )
                sh -x $stepSh > output/$step.txt 2>&1

                awk '
                  BEGIN { content = ""; tag = "'$step-OUTPUT'" }
                  FNR == NR { content = content $0 ORS; next }
                  { gsub(tag, content); print }
                ' output/$step.txt Readme.md > temp.txt && mv temp.txt Readme.md

            done
            """;

    private static File executionFolder;


    @BeforeAll
    public void setUp() throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "docker rm -f $(docker ps -aq)");
        processBuilder.start().waitFor();
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
            for (var file : scenarioFolder.listFiles()) {
                if (isScenario(file)) {
                    Scenario scenario = scenarioYamlConfigReader.readYamlInResources(file.getPath());
                    scenarios.add(Arguments.of(scenario, scenarioFolder));
                }
            }
        }
        return Stream.of(scenarios.toArray(new Arguments[0]));
    }

    @ParameterizedTest
    @MethodSource("sourceForScenario")
    public void testScenario(Scenario scenario, File scenarioFolder) throws Exception {
        executionFolder = createDirectory("target", currentDate, scenarioFolder.getName());

        log.info("Start to test: {} at {}", scenario.getTitle(), executionFolder.getAbsolutePath());

        copyDirectory(scenarioFolder, executionFolder);
        var actions = scenario.getActions();
        createDirectory(executionFolder.getAbsolutePath(), "/asciinema");
        createDirectory(executionFolder.getAbsolutePath(), "/images");
        createDirectory(executionFolder.getAbsolutePath(), "/output");

        appendTo("docker-compose.yaml", getUpdatedDockerCompose(scenario));
        appendTo("run.sh", "");
        appendTo("type.sh", TYPE_SH);
        appendTo("record-output.sh", RECORD_COMMAND_SH);
        appendTo("record-asciinema.sh", RECORD_ASCIINEMA_SH);
        runScenarioSteps(scenario, actions);

        log.info("Re-recording the scenario to include bash commands output in Readme");
        ProcessBuilder commandOutput = new ProcessBuilder();
        commandOutput.directory(executionFolder);
        commandOutput.command("sh", "record-output.sh");
        commandOutput.start().waitFor();

        log.info("Recording one more time with asciinema to be fancy");
        ProcessBuilder recording = new ProcessBuilder();
        recording.directory(executionFolder);
        recording.command("sh", "record-asciinema.sh");
        recording.start().waitFor();

        log.info("Finished to test: {} successfully", scenario.getTitle());
    }

    File createDirectory(String first, String... more) {
        File file = Path.of(first, more).toFile();
        file.mkdirs();
        return file;
    }

    private Scenario scenario;

    private void runScenarioSteps(Scenario scenario, LinkedList<Scenario.Action> actions) throws Exception {
        Map<String, Properties> services = scenario.toServiceProperties();
        this.scenario = scenario;
        try (var clientFactory = new ClientFactory()) {
            int id = 0;
            for (var _action : actions) {
                step(services, clientFactory, ++id, _action);
            }
        }
    }

    private void step(Map<String, Properties> services, ClientFactory clientFactory, int _id, Scenario.Action _action) throws Exception {
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
                String fileContent = readFileToString(new File(executionFolder.getAbsoluteFile() + "/" + action.filename), defaultCharset());
                appendTo("/Readme.md",
                        format("""
                                        ```sh
                                        cat %s
                                        ```

                                        <details%s>
                                          <summary>File content</summary>

                                        ```%s
                                        %s
                                        ```

                                        </details>

                                        """,
                                action.filename,
                                StringUtils.countMatches(fileContent, "\n") < 10 ? " on" : "",
                                getExtension(action.filename),
                                trimToEmpty(fileContent)
                        ));
            }
            case ADD_TOPIC_MAPPING -> {
                var action = ((Scenario.AddTopicMappingAction) _action);

                String gateway = gatewayHost(action);
                String vcluster = vCluster(action);
                TopicMapping topicMapping = TopicMapping
                        .builder()
                        .physicalTopicName(action.getPhysicalTopicName())
                        .build();
                String topicPattern = uriEncode(action.getTopicPattern());

                log.debug("Adding topic mapping in " + vcluster + " for " + topicPattern + " with " + topicMapping);

                given()
                        .baseUri(gateway + "/admin/vclusters/v1")
                        .auth()
                        .basic(ADMIN_USER, ADMIN_PASSWORD)
                        .contentType(ContentType.JSON)
                        .body(topicMapping).
                        when()
                        .post("/vcluster/{vcluster}/topics/{topicPattern}", vcluster, topicPattern).
                        then()
                        .statusCode(SC_OK);

                code(scenario, action, id, """
                                      curl \\
                                        --silent \\
                                        --user "admin:conduktor" \\
                                        --request POST '%s/admin/vclusters/v1/vcluster/%s/topics/%s' \\
                                        --header 'Content-Type: application/json' \\
                                        --data-raw '{
                                            "physicalTopicName": "%s",
                                            "readOnly": false,
                                            "concentrated": true
                                          }' | jq
                                """,
                        gateway,
                        vcluster,
                        topicPattern,
                        action.getPhysicalTopicName());
            }
            case CREATE_VIRTUAL_CLUSTERS -> {
                var action = ((Scenario.CreateVirtualClustersAction) _action);
                String gatewayHost = gatewayHost(action);
                String gatewayBootstrapServers = gatewayBoostrapServers(action);


                String vcluster = action.getName();
                String username = action.getServiceAccount();
                log.debug("Creating virtual cluster " + vcluster + " with service account " + username);
                VClusterCreateResponse response = given()
                        .baseUri(gatewayHost + "/admin/vclusters/v1")
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

                Properties properties = services.getOrDefault(action.getName(), new Properties());
                properties.put(BOOTSTRAP_SERVERS, gatewayBootstrapServers);
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + action.getServiceAccount() + "' password='" + response.getToken() + "';");
                services.put(action.getName(), properties);

                savePropertiesToFile(new File(executionFolder + "/" + action.getName() + "-" + action.getServiceAccount() + ".properties"), properties);

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
                                """,
                        gatewayHost,
                        action.getName(),
                        action.getServiceAccount(),
                        gatewayBootstrapServers,
                        action.getServiceAccount(),
                        action.getName(),
                        action.getServiceAccount());

            }
            case CREATE_TOPICS -> {
                var action = ((Scenario.CreateTopicsAction) _action);

                try (var adminClient = clientFactory.kafkaAdmin(getProperties(services, action))) {
                    for (Scenario.CreateTopicsAction.CreateTopicRequest topic : action.getTopics()) {
                        try {
                            createTopic(adminClient,
                                    topic.getName(),
                                    topic.getPartitions(),
                                    topic.getReplicationFactor(),
                                    topic.getConfig());
                            if (action.isAssertError()) {
                                Assertions.fail("Expected an error during the " + topic.getName() + " creation");
                            }
                        } catch (Exception e) {
                            if (!action.getAssertErrorMessages().isEmpty()) {
                                assertThat(e.getMessage())
                                        .containsIgnoringWhitespaces(action.getAssertErrorMessages().toArray(new String[0]));
                            }
                            if (!action.isAssertError()) {
                                Assertions.fail("Did not expect an error during the " + topic.getName() + " creation", e);
                            }
                            log.warn(topic + " creation failed", e);
                        }
                    }
                }

                String createTopics = "";
                for (Scenario.CreateTopicsAction.CreateTopicRequest topic : action.getTopics()) {
                    createTopics += String.format("""
                                    kafka-topics \\
                                        --bootstrap-server %s%s \\
                                        --replication-factor %s \\
                                        --partitions %s%s \\
                                        --create --if-not-exists \\
                                        --topic %s
                                    """,
                            kafkaBoostrapServers(services, action),
                            action.getKafkaConfig() == null ? "" : " \\\n    --command-config " + action.getKafkaConfig(),
                            "" + topic.getReplicationFactor(),
                            "" + topic.getPartitions(),
                            topic.getConfig()
                                    .entrySet()
                                    .stream()
                                    .map(d -> " \\\n    --config " + d.getKey() + "=" + d.getValue())
                                    .collect(joining()),
                            topic.getName()
                    );

                }
                code(scenario, action, id, createTopics);

            }
            case LIST_TOPICS -> {
                var action = ((Scenario.ListTopicsAction) _action);
                try (var adminClient = clientFactory.kafkaAdmin(getProperties(services, action))) {
                    Set<String> topics = adminClient.listTopics().names().get();
                    code(scenario, action, id, """
                                    kafka-topics \\
                                        --bootstrap-server %s%s \\
                                        --list
                                    """,
                            kafkaBoostrapServers(services, action),
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
                try (var adminClient = clientFactory.kafkaAdmin(getProperties(services, action))) {
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
                                kafkaBoostrapServers(services, action),
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
                try (var producer = clientFactory.kafkaProducer(getProperties(services, action))) {
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
                                                kafkaBoostrapServers(services, action),
                                                action.getKafkaConfig() == null ? "" : " \\\n        --producer.config " + action.getKafkaConfig(),
                                                action.getTopic()))
                                .collect(joining("\n"));
                        code(scenario, action, id, removeEnd(command, "\n"));


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
                Properties properties = getProperties(services, action);
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
                                    --topic %s%s%s%s%s%s
                                """,
                        kafkaBoostrapServers(services, action),
                        action.getKafkaConfig() == null ? "" : " \\\n    --consumer.config " + action.getKafkaConfig(),
                        action.getTopic(),
                        "earliest".equals(properties.get("auto.offset.reset")) ? " \\\n    --from-beginning" : "",
                        action.getMaxMessages() == null ? "" : " \\\n    --max-messages " + maxRecords,
                        action.getAssertSize() == null ? "" : " \\\n    --timeout-ms " + timeout,
                        action.getGroupId() == null ? "" : " \\\n    --group " + action.getGroupId(),
                        !action.isShowHeaders() ? " | jq" : " \\\n    --property print.headers=true"
                );
            }
            case FAILOVER -> {
                var action = ((Scenario.FailoverAction) _action);
                String gatewayHost = gatewayHost(action);
                given()
                        .baseUri(gatewayHost + "/admin/pclusters/v1")
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
                                  --request POST '%s/admin/pclusters/v1/pcluster/%s/switch?to=%s' | jq
                                """,
                        gatewayHost,
                        action.from,
                        action.to);
            }
            case ADD_INTERCEPTORS -> {
                var action = ((Scenario.AddInterceptorAction) _action);
                String gatewayHost = gatewayHost(action);

                configurePlugins(gatewayHost, action.getInterceptors());
                LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors = action.getInterceptors();
                for (var request : interceptors.entrySet()) {
                    LinkedHashMap<String, PluginRequest> plugins = request.getValue();
                    String vcluster = request.getKey();
                    for (var plugin : plugins.entrySet()) {
                        var pluginName = plugin.getKey();
                        var pluginBody = plugin.getValue();

                        appendTo("Readme.md",
                                format("""

                                                Creating the interceptor named `%s` of the plugin `%s` using the following payload

                                                ```json
                                                %s
                                                ```

                                                Here's how to send it:

                                                """,
                                        pluginName,
                                        pluginBody.getPluginClass(),
                                        new ObjectMapper()
                                                .enable(SerializationFeature.INDENT_OUTPUT)
                                                .writeValueAsString(pluginBody)));

                        code(scenario, action, id, """
                                        curl \\
                                            --silent \\
                                            --request POST "%s/admin/interceptors/v1/vcluster/%s/interceptor/%s" \\
                                            --user 'admin:conduktor' \\
                                            --header 'Content-Type: application/json' \\
                                            --data-raw '%s' | jq
                                        """,
                                gatewayHost,
                                vcluster,
                                pluginName,
                                new ObjectMapper().writeValueAsString(pluginBody));
                    }
                }
            }
            case REMOVE_INTERCEPTORS -> {
                var action = ((Scenario.RemoveInterceptorAction) _action);
                String gatewayHost = gatewayHost(action);
                String vcluster = vCluster(action);

                for (String name : action.getNames()) {
                    given()
                            .baseUri(gatewayHost + "/admin/interceptors/v1")
                            .auth()
                            .basic(ADMIN_USER, ADMIN_PASSWORD)
                            .contentType(ContentType.JSON).
                            when()
                            .delete("/vcluster/{vcluster}/interceptor/{pluginName}", vcluster, name).
                            then()
                            .statusCode(SC_NO_CONTENT);
                    code(scenario, action, id, """
                                    curl \\
                                        --silent \\
                                        --request DELETE "%s/admin/interceptors/v1/vcluster/%s/interceptor/%s" \\
                                        --user 'admin:conduktor' \\
                                        --header 'Content-Type: application/json' | jq
                                    """,
                            gatewayHost,
                            vcluster,
                            name);
                }
            }
            case LIST_INTERCEPTORS -> {
                var action = ((Scenario.ListInterceptorAction) _action);
                String gatewayHost = gatewayHost(action);
                String vCluster = vCluster(action);

                TenantInterceptorsResponse response = given()
                        .baseUri(gatewayHost + "/admin/interceptors/v1")
                        .auth()
                        .basic(ADMIN_USER, ADMIN_PASSWORD)
                        .contentType(ContentType.JSON).
                        when()
                        .get("/vcluster/{vcluster}/interceptors", action.vcluster).
                        then()
                        .statusCode(SC_OK)
                        .extract()
                        .response()
                        .as(TenantInterceptorsResponse.class);

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
                                    --header 'Content-Type: application/json' | jq
                                """,
                        gatewayHost,
                        vCluster);
            }
            case DOCKER -> {
                var action = ((Scenario.DockerAction) _action);

                ProcessBuilder processBuilder = new ProcessBuilder();
                processBuilder.directory(executionFolder);
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
                Properties properties = getProperties(services, action);
                String expandedScript = action.getScript();

                var env = new HashMap<String, String>();
                for (String key : properties.stringPropertyNames()) {
                    String formattedKey = key.toUpperCase().replace(".", "_");
                    String value = properties.getProperty(key);
                    expandedScript = replace(expandedScript, "${" + formattedKey + "}", value);
                    env.put(formattedKey, value);
                }

                if (action.getGateway() != null) {
                    expandedScript = replace(expandedScript, "${GATEWAY_HOST}", gatewayHost(action));
                }

                code(scenario, action, id, removeEnd(expandedScript, "\n"));
                File scriptFile = new File(executionFolder + "/step-" + id + ".sh");
                writeStringToFile(scriptFile, ("echo 'Step " + id + " " + action.getType() + " " + trimToEmpty(action.getTitle()) + "'\n") + action.getScript(), defaultCharset());


                ProcessBuilder processBuilder = new ProcessBuilder();
                processBuilder.directory(executionFolder);
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
                Properties properties = services.get(action.getKafka());
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

    private String gatewayHost(Scenario.Action action) {
        assertGatewayHost(action);
        return scenario.getServices().get(action.getGateway())
                .getProperties()
                .get(GATEWAY_HOST);
    }

    private String vCluster(Scenario.GatewayAction action) {
        assertGatewayHost(action);
        if (isBlank(action.vcluster)) {
            throw new RuntimeException(action.simpleMessage() + "gateway has no vcluster specified");
        }
        return action.vcluster;
    }

    private String kafkaBoostrapServers(Map<String, Properties> clusters, Scenario.KafkaAction action) {
        if (isBlank(action.getKafka())) {
            throw new RuntimeException(action.simpleMessage() + "kafka is not specified");
        }
        if (!clusters.containsKey(action.getKafka())) {
            throw new RuntimeException(action.simpleMessage() + " kafka " + action.getKafka() + " is not known");
        }
        if (!clusters.get(action.getKafka()).containsKey(BOOTSTRAP_SERVERS)) {
            throw new RuntimeException(action.simpleMessage() + "kafka " + action.getKafka() + " has not the " + BOOTSTRAP_SERVERS + " property");
        }
        return clusters.get(action.getKafka())
                .getProperty(BOOTSTRAP_SERVERS);
    }

    private String gatewayBoostrapServers(Scenario.Action action) {
        assertGatewayHost(action);
        return scenario.getServices().get(action.getGateway())
                .getProperties()
                .get(BOOTSTRAP_SERVERS);
    }

    private void assertGatewayHost(Scenario.Action action) {
        if (isBlank(action.getGateway())) {
            throw new RuntimeException(action.simpleMessage() + "gateway is not specified");
        }
        Scenario.Service service = scenario.getServices().get(action.getGateway());
        if (service == null) {
            throw new RuntimeException(action.simpleMessage() + "gateway " + action.getGateway() + " is not known");
        }
        String gatewayHost = service.getProperties().get(GATEWAY_HOST);
        if (gatewayHost == null) {
            throw new RuntimeException(action.simpleMessage() + "gateway " + action.getGateway() + " has not the " + GATEWAY_HOST + " property");
        }
        if (!gatewayHost.startsWith("http")) {
            throw new RuntimeException(action.simpleMessage() + "gateway " + action.getGateway() + " has not the " + GATEWAY_HOST + " property should start with 'http', it is " + gatewayHost);
        }
    }

    private void savePropertiesToFile(File propertiesFile, Properties properties) throws IOException {
        String content = properties.keySet().stream().map(key -> key + "=" + properties.get(key)).collect(joining("\n"));
        writeStringToFile(propertiesFile, content, defaultCharset());
    }

    private Properties getProperties(Map<String, Properties> services, Scenario.KafkaAction action) {
        if (isBlank(action.getKafka())) {
            throw new RuntimeException(action.simpleMessage() + " needs to define its target kafka");
        }
        Properties kafkaService = services.get(action.getKafka());
        if (kafkaService == null) {
            throw new RuntimeException(action.simpleMessage() + " has a kafka as " + action.getKafka() + ", it is not known");
        }
        Properties p = new Properties();
        p.putAll(kafkaService);
        if (action.getProperties() != null) {
            p.putAll(action.getProperties());
        }
        return p;
    }

    private static void createTopic(AdminClient kafkaAdminClient, String topic, int partitions, int replicationFactor, Map<String, String> config) throws InterruptedException {
        KafkaActionUtils.createTopic(kafkaAdminClient, topic, partitions, (short) replicationFactor, config, 10);
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class TenantInterceptorsResponse {
        List<PluginResponse> interceptors;
    }

    private static void assertRecords(List<ConsumerRecord<String, String>> records, List<Scenario.RecordAssertion> recordAssertions) {

        List<String> keys = records.stream().map(ConsumerRecord::key).toList();
        List<String> values = records.stream().map(ConsumerRecord::value).toList();
        List<Header> headers = records.stream().flatMap(r -> getHeaders(r).stream()).toList();


        for (Scenario.RecordAssertion recordAssertion : recordAssertions) {
            boolean validKey = validate(recordAssertion.getKey(), keys);
            boolean validValues = validate(recordAssertion.getValue(), values);
            boolean validHeader = validateHeaders(recordAssertion, headers);
            if (isNotBlank(recordAssertion.getDescription())) {
                log.info("Test: " + recordAssertion.getDescription());
            }
            if ((validKey && validValues && validHeader) == false) {
                log.info("Assertion failed with key: " + validKey + ", values: " + validValues + ", header: " + validHeader);
                Assertions.fail(recordAssertion.getDescription() + " failed");
            }
        }
    }

    private static boolean validateHeaders(Scenario.RecordAssertion recordAssertion, List<Header> headers) {
        if (recordAssertion.getHeaders().isEmpty()) {
            return true;
        }

        //recordAssertion.getHeaderKeys().
        for (Scenario.Assertion headerKeyAssertion : recordAssertion.getHeaderKeys()) {
            headers.stream().filter(header -> validate(headerKeyAssertion, header.key())).findFirst().isPresent();
        }

        for (String headerKey : recordAssertion.getHeaders().keySet()) {
            Scenario.Assertion headerAssertion = recordAssertion.getHeaders().get(headerKey);
            List<String> headerValues = headers.stream().filter(e -> headerKey.equals(e.key())).map(h -> new String(h.value())).toList();
            if (!validate(headerAssertion, headerValues)) {
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
            case "isBlank" -> isBlank(data);
            case "isNotBlank" -> isNotBlank(data);
            case "containsIgnoreCase" -> containsIgnoreCase(data, expected);
            case "contains" -> contains(data, expected);
            case "doesNotContain" -> !contains(data, expected);
            case "doesNotContainIgnoringCase" -> !containsIgnoreCase(data, expected);
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

        String step = "step-" + id + "-" + action.getType();
        appendTo("run.sh", "echo '" + action.getTitle() + "'\nsh " + step + ".sh\n\n");
        appendTo(step + ".sh", format(format, args));

        appendTo("/Readme.md",
                format("""
                                ```sh
                                %s
                                ```

                                <details>
                                  <summary>Realtime command output</summary>

                                  ![%s](images/%s.gif)

                                </details>

                                <details>
                                  <summary>Command output</summary>

                                ```sh
                                %s-OUTPUT
                                ```

                                </details>

                                """,
                        StringUtils.removeEnd(format(format, args), "\n"),
                        action.getTitle(),
                        step,
                        step
                ));
    }

    private static void appendTo(String filename, String code) throws IOException {
        File file = new File(executionFolder + "/" + filename);
        if ("sh".equals(getExtension(file.getName())) && !code.startsWith("#!/bin/")) {
            if ((file.exists() && !readFileToString(file, defaultCharset()).startsWith("#!/bin/"))) {
                code = "#!/bin/sh\n" + code;
            }
        }
        writeStringToFile(
                file,
                code,
                defaultCharset(),
                true);
        if ("sh".equals(getExtension(file.getName()))) {
            file.setExecutable(true);
        }
    }


    public static String uriEncode(String s) throws Exception {
        return URLEncoder.encode(s, "UTF-8").replace("*", "%2A");

    }
}
