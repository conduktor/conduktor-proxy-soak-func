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
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.*;
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

    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "conduktor";
    private final static String SCENARIO_DIRECTORY_PATH = "config/scenario";
    public static final String GATEWAY_HOST = "gateway.host";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    static final String currentDate = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss").format(Calendar.getInstance().getTime());

    public static final String TYPE_SH = """
            function type_and_execute() {
              local GREEN="\\033[0;32m"
              local WHITE='\\033[0;97m'
              local RESET="\\033[0m"
              local file="$1"
              local chars=$(cat $file| wc -c)

              printf "${WHITE}"
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

              sh $file
            }

            type_and_execute "$1"
            """;
    public static final String RECORD_ASCIINEMA_SH = """
            for stepSh in $(ls step*sh | sort ) ; do
                echo "Processing asciinema for $stepSh " `date`
                step=$(echo "$stepSh" | sed "s/.sh$//" )
                rows=20

                asciinema rec \\
                  --title "$step" \\
                  --idle-time-limit 2 \\
                  --cols 140 --rows $rows \\
                  --command "sh type.sh $step.sh" \\
                  --overwrite \\
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

            docker rm -f $(docker ps -aq)
            docker compose up -d --wait

            asciinema rec \\
              --title "%s" \\
              --idle-time-limit 2 \\
              --cols 140 --rows 40 \\
              --command "sh run.sh" \\
              --overwrite \\
              asciinema/all.asciinema

            svg-term \\
              --in asciinema/all.asciinema \\
              --width 140 --height 20 \\
              --out images/all.svg \\
              --window true

            asciinemaUid=$(asciinema upload asciinema/all.asciinema 2>&1 | grep http | awk '{print $1}' | cut -d '/' -f 5)
            gsed -i "s/ASCIINEMA_UID/$asciinemaUid/g" Readme.md
                        
            markers=""
            step=1
            for time in $(grep "#" asciinema/all.asciinema | cut -d "." -f 1 | sed "s/\\[//g") ; do
              stepTitle=`grep "execute.*sh" run.sh | awk "NR==$step" |cut -d '"' -f 4 | tr -d '\\'`
              echo $stepTitle
              markers=""\"$markers
            $time.0 - $step - $stepTitle""\"
              step=$((step+1))
            done
                        
                        
            curl "https://asciinema.org/a/$asciinemaUid" \\
              -H 'authority: asciinema.org' \\
              -H 'content-type: application/x-www-form-urlencoded' \\
              -H 'cookie: a608749=1; a608760=1; a608768=1; auth_token=E1WbYRLAwFWjtpBumGG5; a608770=1; a608897=1; a608906=1; _asciinema_key=SFMyNTY.g3QAAAACbQAAAAtfY3NyZl90b2tlbm0AAAAYRTNXN1cteG1hT1h4T1l5amFFZkN2Y0lZbQAAAAd1c2VyX2lkYgABp3M.00p16kmv-MCNeoQG_CCcdMNEIcbeoX7Sz4LW9f-gwcQ' \\
              -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36' \\
              --data-raw '_method=put&_csrf_token=I0cIAgAZEzQ1GwArGCAdXA4uI3sQAS4gft_5W4kYTTXSWyd6okE8fbgy' \\
              --data-urlencode "asciicast[markers]=$markers"

            """;
    public static final String RECORD_COMMAND_SH = """
            for stepSh in $(ls step*sh | sort ) ; do
                echo "Processing $stepSh " `date`
                step=$(echo "$stepSh" | sed "s/.sh$//" )
                sh -x $stepSh > output/$step.txt
                echo " " >> output/$step.txt

                awk '
                  BEGIN { content = ""; tag = "'$step-OUTPUT'" }
                  FNR == NR { content = content $0 ORS; next }
                  { gsub(tag, content); print }
                ' output/$step.txt Readme.md > temp.txt && mv temp.txt Readme.md

            done
            """;
    public static final String KAFKA_CONFIG_FILE = "KAFKA_CONFIG_FILE";
    public static final ObjectMapper PRETTY_OBJECT_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static File executionFolder;


    @BeforeEach
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
                    if (scenario.isEnabled()) {
                        scenarios.add(Arguments.of(scenario, scenarioFolder));
                    } else {
                        log.warn("Scenario {} is not enabled", scenario.getTitle());
                    }
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
        appendTo("run.sh", """
                #!/bin/sh

                RED='\\033[0;31m'
                GREEN='\\033[0;32m'
                YELLOW='\\033[0;33m'
                BLUE='\\033[0;34m'
                WHITE='\\033[0;97m'
                NC='\\033[0m' # No Color

                function banner() {
                    printf "$1# $2$NC\\n" | pv -qL 20
                }

                function header() {
                    banner "$RED" "$1"
                }

                function step() {
                    banner "$BLUE" "$1"
                }
                                
                function execute() {
                    local script=$1
                    local title=$2
                    step "$title"
                    sh type.sh "$script"
                    echo
                }
                                

                """);
        appendTo("type.sh", TYPE_SH);
        appendTo("record-output.sh", RECORD_COMMAND_SH);
        appendTo("record-asciinema.sh", format(RECORD_ASCIINEMA_SH, scenario.getTitle()));
        runScenarioSteps(scenario, actions);

        if (scenario.isRecordOutput()) {
            executeSh(true, "Recording outputs", "sh", "record-output.sh");
        }

        if (scenario.isRecordAscinema()) {
            executeSh(true, "Recording asciinema", "sh", "record-asciinema.sh");
        }

        log.info("Finished to test: {} successfully", scenario.getTitle());
    }

    private void executeSh(boolean showOutput, String description, String... command) throws IOException, InterruptedException {
        log.info("{} {}", description, String.join(" ", command));
        ProcessBuilder recording = new ProcessBuilder();
        recording.directory(executionFolder);
        recording.redirectErrorStream(true);
        recording.command(command);
        Process process = recording.start();
        if (showOutput) {
            showProcessOutput(process);
        }
        process.waitFor();
    }

    private void showProcessOutput(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
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
        appendTo("run.sh", "header '" + scenario.getTitle() + "'\n");
        try (var clientFactory = new ClientFactory()) {
            int id = 0;
            for (var _action : actions) {
                step(services, clientFactory, ++id, _action);
            }
        }
    }

    private void step(Map<String, Properties> services, ClientFactory clientFactory, int _id, Scenario.Action _action) throws Exception {
        String id = format("%02d", _id);
        if (!_action.isEnabled()) {
            log.warn("[" + id + "] skipping " + _action.simpleMessage());
            return;
        }
        log.info("[" + id + "] " + _action.simpleMessage());

        appendTo("Readme.md",
                format("""
                                %s

                                %s

                                """,
                        _action.markdownHeader(),
                        trimToEmpty(_action.getMarkdown())));

        switch (_action.getType()) {
            case CONCLUSION, STEP, ASCIINEMA -> {
                //
            }
            case INTRODUCTION -> {
                executeSh(false, "Starting docker behind the scene to have a smooth recording",
                        "docker", "compose", "up", "--detach", "--wait");
            }
            case FILE -> {
                var action = ((Scenario.FileAction) _action);
                String fileContent = readFileToString(new File(executionFolder.getAbsoluteFile() + "/" + action.getFilename()), defaultCharset());
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
                                action.getFilename(),
                                countMatches(fileContent, "\n") < 10 ? " on" : "",
                                getExtension(action.getFilename()),
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
                        .concentrated(true)
                        .build();
                String topicPattern = uriEncode(action.getTopicPattern());

                log.debug("Adding topic mapping in " + vcluster + " for " + topicPattern + " with " + topicMapping);

                Response post = given()
                        .baseUri(gateway + "/admin/vclusters/v1")
                        .auth()

                        .basic(ADMIN_USER, ADMIN_PASSWORD)
                        .contentType(ContentType.JSON)
                        .body(topicMapping).
                        when()
                        .urlEncodingEnabled(false)
                        .post("/vcluster/{vcluster}/topics/{topicPattern}", vcluster, topicPattern);
                post.
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

                Properties serviceProperty = services.getOrDefault(action.getName(), new Properties());
                Properties properties = new Properties(serviceProperty);
                properties.put(BOOTSTRAP_SERVERS, gatewayBootstrapServers);
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + action.getServiceAccount() + "' password='" + response.getToken() + "';");
                if (!services.containsKey(action.getName())) {
                    // by convention setup the config with the first cluster credentials of the same name
                    services.put(action.getName(), properties);
                }

                savePropertiesToFile(new File(executionFolder + "/" + action.getName() + "-" + action.getServiceAccount() + ".properties"), properties);

                code(scenario, action, id,
                        """
                                token=$(curl \\
                                    --silent \\
                                    --user 'admin:conduktor' \\
                                    --request POST "%s/admin/vclusters/v1/vcluster/%s/username/%s" \\
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

                var adminClient = clientFactory.kafkaAdmin(getProperties(services, action));
                for (Scenario.CreateTopicsAction.CreateTopicRequest topic : action.getTopics()) {
                    try {
                        createTopic(adminClient,
                                topic.getName(),
                                topic.getPartitions(),
                                topic.getReplicationFactor(),
                                topic.getConfig());
                        if (action.getAssertError()) {
                            Assertions.fail("Expected an error during the " + topic.getName() + " creation");
                        }
                    } catch (Exception e) {
                        if (!action.getAssertErrorMessages().isEmpty()) {
                            assertThat(e.getMessage())
                                    .containsIgnoringWhitespaces(action.getAssertErrorMessages().toArray(new String[0]));
                        }
                        if (!action.getAssertError()) {
                            Assertions.fail("Did not expect an error during the " + topic.getName() + " creation", e);
                        }
                        log.warn(topic + " creation failed", e);
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
                var adminClient = clientFactory.kafkaAdmin(getProperties(services, action));
                Set<String> topics = adminClient.listTopics().names().get();
                code(scenario, action, id, """
                                kafka-topics \\
                                    --bootstrap-server %s%s \\
                                    --list
                                """,
                        kafkaBoostrapServers(services, action),
                        action.getKafkaConfig() == null ? "" : " \\\n    --command-config " + action.getKafkaConfig());
                if (Objects.nonNull(action.getAssertSize())) {
                    assertThat(topics)
                            .hasSize(action.getAssertSize());
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
            case ALTER_TOPICS -> {
                var action = ((Scenario.AlterTopicAction) _action);
                var adminClient = clientFactory.kafkaAdmin(getProperties(services, action));
                for (Scenario.AlterTopicAction.AlterTopicRequest topic : action.getTopics()) {
                    try {
                        List<ConfigEntry> configEntries = topic
                                .getConfig()
                                .entrySet()
                                .stream()
                                .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
                                .toList();

                        Map<ConfigResource, Config> configs = Map.of(
                                new ConfigResource(ConfigResource.Type.TOPIC, topic.getName()),
                                new Config(configEntries));
                        adminClient.alterConfigs(configs)
                                .values()
                                .values()
                                .forEach(e -> {
                                    try {
                                        e.get();
                                    } catch (Exception ex) {
                                        throw new RuntimeException(ex);
                                    }
                                });


                        code(scenario, action, id, """
                                        kafka-topics \\
                                            --bootstrap-server %s%s%s \\
                                            --alter \\
                                            --topic %s
                                        """,
                                kafkaBoostrapServers(services, action),
                                action.getKafkaConfig() == null ? "" : " \\\n    --command-config " + action.getKafkaConfig(),
                                configEntries
                                        .stream()
                                        .map(d -> " \\\n    --add-config " + d.name() + "=" + d.value())
                                        .collect(joining("")),
                                topic.getName());
                    } catch (Exception e) {
                        if (!action.getAssertErrorMessages().isEmpty()) {
                            assertThat(e.getMessage())
                                    .containsIgnoringWhitespaces(action.getAssertErrorMessages().toArray(new String[0]));
                        }
                        if (!action.getAssertError()) {
                            Assertions.fail("Did not expect an error during update of " + topic.getName() + " ", e);
                        }
                        log.warn(topic + " update failed", e);

                    }
                }

            }
            case DESCRIBE_TOPICS -> {
                var action = ((Scenario.DescribeTopicsAction) _action);
                var adminClient = clientFactory.kafkaAdmin(getProperties(services, action));
                Map<String, TopicDescription> topics = adminClient
                        .describeTopics(action.getTopics())
                        .allTopicNames()
                        .get();
                for (String topic : action.getTopics()) {
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
            case PRODUCE -> {
                var action = ((Scenario.ProduceAction) _action);
                Properties properties = getProperties(services, action);
                properties.put(ProducerConfig.ACKS_CONFIG, action.getAcks() == null ? "1" : action.getAcks());
                properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, action.getCompression() == null ? "none" : action.getCompression());

                var producer = clientFactory.kafkaProducer(properties);
                try {
                    produce(action.getTopic(), action.getMessages(), producer);


                    String command = action.getMessages()
                            .stream()
                            .map(message ->
                                    format("""
                                                    echo '%s' | \\
                                                        kafka-console-producer \\
                                                            --bootstrap-server %s%s%s%s \\
                                                            --topic %s
                                                    """,
                                            message.getValue(),
                                            kafkaBoostrapServers(services, action),
                                            action.getKafkaConfig() == null ? "" : " \\\n        --producer.config " + action.getKafkaConfig(),
                                            action.getAcks() == null ? "" : " \\\n        --request-required-acks  " + action.getAcks(),
                                            action.getCompression() == null ? "" : " \\\n        --compression-codec  " + action.getCompression(),
                                            action.getTopic()))
                            .collect(joining("\n"));
                    code(scenario, action, id, removeEnd(command, "\n"));


                    System.out.println(action);
                    if (action.isAssertError()) {
                        Assertions.fail("Produce should have failed");
                    }
                } catch (Exception e) {
                    if (!action.getAssertErrorMessages().isEmpty()) {
                        assertThat(e.getMessage())
                                .contains(action.getAssertErrorMessages());
                    }
                    if (!action.isAssertError()) {
                        Assertions.fail("Could not produce message on " + action.getTopic(), e);
                    }
                    log.error("could not produce message ", e);
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
                if (!properties.contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                }
                final long timeout;
                if (action.getTimeout() != null) {
                    timeout = action.getTimeout();
                } else if (action.getAssertSize() != null || action.getMaxMessages() == null) {
                    timeout = TimeUnit.SECONDS.toMillis(5);
                } else {
                    timeout = TimeUnit.SECONDS.toMillis(15);
                }

                int maxRecords;
                if (action.getMaxMessages() != null) {
                    maxRecords = action.getMaxMessages();
                } else if (action.getAssertSize() != null) {
                    maxRecords = action.getAssertSize() + 1;
                } else {
                    maxRecords = 100;
                }

                var consumer = clientFactory.consumer(properties);
                consumer.subscribe(Arrays.asList(action.getTopic()));
                int recordCount = 0;
                long startTime = System.currentTimeMillis();
                var records = new ArrayList<ConsumerRecord<String, String>>();

                while (recordCount < maxRecords || (action.getAssertSize() != null && recordCount > action.getAssertSize())) {
                    if (!(System.currentTimeMillis() < startTime + timeout)) break;
                    var consumedRecords = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                    recordCount += consumedRecords.count();
                    for (var record : consumedRecords) {
                        if (action.getShowRecords()) {
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
                        !action.getShowHeaders() ? " | jq" : " \\\n    --property print.headers=true"
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
                        .post("/pcluster/{from}/switch?to={to}", action.getFrom(), action.getTo())
                        .then()
                        .statusCode(SC_OK)
                        .extract()
                        .response();

                code(scenario, action, id, """
                                curl \\
                                  --silent \\
                                  --request POST '%s/admin/pclusters/v1/pcluster/%s/switch?to=%s' \\
                                  --user "admin:conduktor" | jq
                                """,
                        gatewayHost,
                        action.getFrom(),
                        action.getTo());

                appendTo("/Readme.md",
                        format("""
                                        From now on `%s` the cluster with id `%s` is pointing to the `%s cluster.

                                        """,
                                action.getGateway(),
                                action.getFrom(),
                                action.getTo()
                        ));
            }
            case ADD_INTERCEPTORS -> {
                var action = ((Scenario.AddInterceptorAction) _action);
                String gatewayHost = gatewayHost(action);

                configurePlugins(gatewayHost, action.getInterceptors(), action.getUsername());
                LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors = action.getInterceptors();
                for (var request : interceptors.entrySet()) {
                    LinkedHashMap<String, PluginRequest> plugins = request.getValue();
                    String vcluster = request.getKey();
                    for (var plugin : plugins.entrySet()) {
                        var pluginName = plugin.getKey();
                        var pluginBody = plugin.getValue();

                        appendTo("Readme.md",
                                format("""

                                                Creating the interceptor named `%s` of the plugin `%s`%s using the following payload

                                                ```json
                                                %s
                                                ```

                                                Here's how to send it:

                                                """,
                                        pluginName,
                                        action.getUsername() == null ? "" : " for " + action.getUsername(),
                                        pluginBody.getPluginClass(),
                                        PRETTY_OBJECT_MAPPER
                                                .writeValueAsString(pluginBody)));

                        code(scenario, action, id, """
                                        curl \\
                                            --silent \\
                                            --request POST "%s/admin/interceptors/v1/vcluster/%s%s/interceptor/%s" \\
                                            --user 'admin:conduktor' \\
                                            --header 'Content-Type: application/json' \\
                                            --data-raw '%s' | jq
                                        """,
                                gatewayHost,
                                vcluster,
                                action.getUsername() == null ? "" : "/username/" + action.getUsername(),
                                pluginName,
                                OBJECT_MAPPER.writeValueAsString(pluginBody)
                        );
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
                        .get("/vcluster/{vcluster}/interceptors", action.getVcluster()).
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
                                    --silent \\
                                    --request GET "%s/admin/interceptors/v1/vcluster/%s/interceptors" \\
                                    --user "admin:conduktor" \\
                                    --header 'Content-Type: application/json' | jq
                                """,
                        gatewayHost,
                        vCluster);
            }
            case DOCKER -> {
                var action = ((Scenario.DockerAction) _action);

                if (action.getKafka() != null) {
                    Properties properties = getProperties(services, action);
                    var keysToRemove = new ArrayList<String>();
                    for (String envKey : action.getEnvironment().keySet()) {
                        boolean found = false;
                        for (String key : properties.stringPropertyNames()) {
                            String formattedKey = key.toUpperCase().replace(".", "_");
                            String value = properties.getProperty(key);
                            String searchString = "${" + formattedKey + "}";
                            String searchStringWithDefault = "${" + formattedKey + ":-}";
                            if (action.getEnvironment().get(envKey).contains(searchString) || action.getEnvironment().get(envKey).contains(searchStringWithDefault)) {
                                action.getEnvironment().put(envKey, replace(action.getEnvironment().get(envKey), searchString, value));
                                action.getEnvironment().put(envKey, replace(action.getEnvironment().get(envKey), searchStringWithDefault, value));
                                found = true;
                            }
                        }
                        if (found == false) {
                            keysToRemove.add(envKey);
                        }
                    }
                    keysToRemove.forEach(key -> action.getEnvironment().remove(key));
                }

                ProcessBuilder processBuilder = new ProcessBuilder();
                processBuilder.directory(executionFolder);
                processBuilder.environment().putAll(action.getEnvironment());

                String command = ((Scenario.CommandAction) action).getCommand();
                if (action.isDaemon()) {
                    processBuilder.command("sh", "-c", "nohup " + command + "&");
                } else {
                    if (command.equals("docker compose down --volumes")) {
                        processBuilder.command("sh", "-c", "docker rm -f $(docker ps -aq) ; " + command);
                    } else {
                        processBuilder.command("bash", "-c", command);
                    }
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
                        command);

            }
            case SH -> {
                var action = ((Scenario.ShAction) _action);
                String expandedScript = action.getScript();
                var env = new HashMap<String, String>();
                if (action.getKafka() != null) {
                    Properties properties = getProperties(services, action);
                    if (isNotBlank(action.getKafkaConfig())) {
                        properties.put(KAFKA_CONFIG_FILE, action.getKafkaConfig());
                    }

                    for (String key : properties.stringPropertyNames()) {
                        String formattedKey = key.toUpperCase().replace(".", "_");
                        String value = properties.getProperty(key);
                        expandedScript = replace(expandedScript, "${" + formattedKey + "}", value);
                        env.put(formattedKey, value);
                    }
                }

                if (action.getGateway() != null) {
                    expandedScript = replace(expandedScript, "${GATEWAY_HOST}", gatewayHost(action));
                }

                code(scenario, action, id, removeEnd(expandedScript, "\n"));

                for (int iteration = 1; iteration <= action.getIteration(); iteration++) {
                    String script = "step-" + id + "-" + action.getType() + ".sh";
                    if (iteration > 1) {
                        log.info("Iteration {}/{} for {}", iteration, action.getIteration(), script);
                    }
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.directory(executionFolder);
                    processBuilder.command("sh", script);
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
        if (isBlank(action.getVcluster())) {
            try {
                System.out.println(PRETTY_OBJECT_MAPPER
                        .writeValueAsString(action));
            } catch (Exception e) {
                //
            }
            throw new RuntimeException("[" + action.simpleMessage() + "] gateway has no vcluster specified");
        }
        return action.getVcluster();
    }

    private String kafkaBoostrapServers(Map<String, Properties> clusters, Scenario.KafkaAction action) {
        if (isBlank(action.getKafka())) {
            throw new RuntimeException(action.simpleMessage() + "kafka is not specified");
        }
        if (!clusters.containsKey(action.getKafka())) {
            throw new RuntimeException("[" + action.simpleMessage() + "] kafka " + action.getKafka() + " is not known");
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

    private Properties getProperties(Map<String, Properties> services, Scenario.KafkaAction action) throws Exception {
        if (isBlank(action.getKafka())) {
            throw new RuntimeException("[" + action.simpleMessage() + "] needs to define its target kafka");
        }
        Properties kafkaService = services.get(action.getKafka());
        if (kafkaService == null) {
            throw new RuntimeException("[" + action.simpleMessage() + "] has a kafka as " + action.getKafka() + ", it is not known");
        }
        Properties p = new Properties();
        p.putAll(kafkaService);
        if (action.getProperties() != null) {
            p.putAll(action.getProperties());
        }
        if (action.getKafkaConfig() != null) {
            Properties specifiedManually = new Properties();
            specifiedManually.load(new FileInputStream(executionFolder + "/" + action.getKafkaConfig()));
            // override with specified
            p.putAll(specifiedManually);
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
            ProducerRecord<String, String> recordRequest = KafkaActionUtils.record(topic, message.getKey(), message.getValue(), (List<Header>) inputHeaders);
            var recordMetadata = producer.send(recordRequest).get();
            assertThat(recordMetadata.hasOffset()).isTrue();
        }
    }


    private static void configurePlugins(String gateway, LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> plugins, String username) {
        for (var plugin : plugins.entrySet()) {
            for (var plugin1 : plugin.getValue().entrySet()) {
                var pluginName = plugin1.getKey();
                log.info("Configuring " + pluginName + " " + (username == null ? "" : " for " + username));
                var pluginBody = plugin1.getValue();
                RequestSpecification when = given()
                        .baseUri(gateway + "/admin/interceptors/v1")
                        .auth()
                        .basic(ADMIN_USER, ADMIN_PASSWORD)
                        .body(pluginBody)
                        .contentType(ContentType.JSON)
                        .when();
                final Response post;
                if (username == null) {
                    post = when.post("/vcluster/{vcluster}/interceptor/{pluginName}", plugin.getKey(), pluginName);
                } else {
                    post = when.post("/vcluster/{vcluster}/username/{username}/interceptor/{pluginName}", plugin.getKey(), username, pluginName);
                }
                post
                        .then()
                        .statusCode(SC_CREATED)
                        .extract()
                        .response();
            }
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

        String keys = records.stream().map(ConsumerRecord::key).collect(joining("\n"));
        String values = records.stream().map(ConsumerRecord::value).collect(joining("\n"));
        List<Header> headers = records.stream().flatMap(r -> getHeaders(r).stream()).toList();


        for (Scenario.RecordAssertion recordAssertion : recordAssertions) {
            boolean validKey = validate(recordAssertion.getKey(), keys);
            boolean validValues = validate(recordAssertion.getValue(), values);
            boolean validHeader = validateHeaders(recordAssertion.getHeaders(), headers);
            if (isNotBlank(recordAssertion.getDescription())) {
                log.info("Test: " + recordAssertion.getDescription());
            }
            if (!(validKey && validValues && validHeader)) {
                Assertions.fail(
                        String.format("""
                                        Failure %s
                                                                                
                                        values: %s %s
                                        key: %s %s
                                        header: %s %s
                                        """,
                                recordAssertion.getDescription(),
                                "" + validValues,
                                validValues ? "" : String.join(values, ","),
                                validKey,
                                validKey ? "" : String.join(keys, ","),
                                validHeader,
                                validHeader ? "" : headers));
            }
        }
    }

    private static boolean validateHeaders(LinkedHashMap<String, Scenario.Assertion> assertions, List<Header> headers) {
        if (assertions.isEmpty()) {
            return true;
        }

        String headersAsString = headers
                .stream()
                .map(e -> e.key() + ":" + (e.value() == null ? "" : e.value()))
                .collect(joining(", "));

        boolean valid = true;
        for (String headerKey : assertions.keySet()) {
            Scenario.Assertion assertion = new Scenario.Assertion();
            assertion.setOperator(assertions.get(headerKey).getOperator());
            assertion.setExpected(headerKey + ":" + (assertion.getExpected() == null ? "" : assertion.getExpected()));
            if (!validate(assertion, headersAsString)) {
                log.warn(assertion + " failed");
                valid = false;
            }
        }
        if (!valid) {
            log.warn("Header validation failed, input was [" + headersAsString + "]");
        }
        return valid;
    }

    public static boolean validate(Scenario.Assertion assertion, String data) {
        if (assertion == null) {
            return true;
        }
        String expected = assertion.getExpected();
        return switch (assertion.getOperator()) {
            case "satisfies" -> satisfies(data, expected);
            case "isBlank" -> isBlank(data);
            case "isNotBlank" -> isNotBlank(data);
            case "isEqualTo" -> expected.equals(data);
            case "isEqualToIgnoreCase" -> equalsIgnoreCase(expected, data);
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
            var dataAsMap = OBJECT_MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {
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
        appendTo("run.sh", format("""
                        execute "%s.sh" "%s"
                        """,
                step,
                action.getTitle().replace("`", "\\`")));
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
                        removeEnd(format(format, args), "\n"),
                        action.getTitle(),
                        step,
                        step
                ));
    }

    private static void appendTo(String filename, String code) throws IOException {
        File file = new File(executionFolder + "/" + filename);
        if ("sh".equals(getExtension(file.getName())) && !code.startsWith("#!/bin/")) {
            if ((file.exists() && !readFileToString(file, defaultCharset()).startsWith("#!/bin/"))) {
                code = "\n" + code;
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
