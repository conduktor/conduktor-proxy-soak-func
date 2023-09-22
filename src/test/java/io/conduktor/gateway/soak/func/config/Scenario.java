package io.conduktor.gateway.soak.func.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Scenario {
    protected String title;
    protected boolean enabled = true;
    protected boolean recordAscinema = true;
    protected boolean recordOutput = true;
    private Map<String, Service> services;
    protected LinkedList<Action> actions;

    public Map<String, Properties> toServiceProperties() {
        Map<String, Properties> ret = new HashMap<>();
        getServices().forEach((name, s) -> ret.put(name, s.toProperties()));
        return ret;
    }

    @Override
    public String toString() {
        return getTitle();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Service {
        private Map<String, Object> docker;
        protected LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        protected LinkedHashMap<String, String> environment = new LinkedHashMap<>();

        public Properties toProperties() {
            Properties ret = new Properties();
            ret.putAll(properties);
            return ret;
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type",
            visible = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ProduceAction.class, name = "PRODUCE"),
            @JsonSubTypes.Type(value = ConsumeAction.class, name = "CONSUME"),
            @JsonSubTypes.Type(value = CreateTopicsAction.class, name = "CREATE_TOPICS"),
            @JsonSubTypes.Type(value = AlterTopicAction.class, name = "ALTER_TOPICS"),
            @JsonSubTypes.Type(value = CreateVirtualClustersAction.class, name = "CREATE_VIRTUAL_CLUSTERS"),
            @JsonSubTypes.Type(value = ListTopicsAction.class, name = "LIST_TOPICS"),
            @JsonSubTypes.Type(value = DescribeTopicsAction.class, name = "DESCRIBE_TOPICS"),
            @JsonSubTypes.Type(value = AddInterceptorAction.class, name = "ADD_INTERCEPTORS"),
            @JsonSubTypes.Type(value = RemoveInterceptorAction.class, name = "REMOVE_INTERCEPTORS"),
            @JsonSubTypes.Type(value = AddTopicMappingAction.class, name = "ADD_TOPIC_MAPPING"),
            @JsonSubTypes.Type(value = ListInterceptorAction.class, name = "LIST_INTERCEPTORS"),
            @JsonSubTypes.Type(value = DocumentationAction.class, name = "DOCUMENTATION"),
            @JsonSubTypes.Type(value = FileAction.class, name = "FILE"),
            @JsonSubTypes.Type(value = IntroductionAction.class, name = "INTRODUCTION"),
            @JsonSubTypes.Type(value = AsciinemaAction.class, name = "ASCIINEMA"),
            @JsonSubTypes.Type(value = ConclusionAction.class, name = "CONCLUSION"),
            @JsonSubTypes.Type(value = ShAction.class, name = "SH"),
            @JsonSubTypes.Type(value = StepAction.class, name = "STEP"),
            @JsonSubTypes.Type(value = DescribeKafkaPropertiesAction.class, name = "DESCRIBE_KAFKA_PROPERTIES"),
            @JsonSubTypes.Type(value = DockerAction.class, name = "DOCKER"),
            @JsonSubTypes.Type(value = FailoverAction.class, name = "FAILOVER")
    })
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        protected ActionType type;
        protected Integer headerLevel;
        protected String title = "";
        public String markdown = "";
        public String gateway;
        public boolean enabled = true;

        public String simpleMessage() {
            return type + " " + getTitle();
        }

        public String markdownHeader() {
            return StringUtils.repeat("#", getHeaderLevel()) + " " + trimToEmpty(getTitle());
        }

        public int getHeaderLevel() {
            if (headerLevel != null) {
                return headerLevel;
            }
            return getTitle().toLowerCase().startsWith("review") ? 3 : 2;
        }
    }

    @Data
    public static class KafkaAction extends Action {
        protected String kafka;
        protected String kafkaConfig;
        protected LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    }

    @Data
    public static class DockerAction extends CommandAction {
        protected String command;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return switch (command) {
                case "docker compose down --volumes" -> "Cleanup the docker environment";
                case "docker compose up --detach --wait" -> "Startup the docker environment";
                default -> "Execute `" + command + "`";
            };
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            return switch (command) {
                case "docker compose down --volumes" -> """
                        Remove all your docker processes and associated volumes

                        * `--volumes`: Remove named volumes declared in the "volumes" section of the Compose file and anonymous volumes attached to containers.
                        """;
                case "docker compose up --detach --wait" -> """
                        Start all your docker processes, wait for them to be up and ready, then run in background

                        * `--wait`: Wait for services to be `running|healthy`. Implies detached mode.
                        * `--detach`: Detached mode: Run containers in the background
                        """;
                default -> null;
            };
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ListTopicsAction extends KafkaAction {
        protected Integer assertSize;
        protected List<String> assertExists = List.of();
        protected List<String> assertDoesNotExist = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Listing topics in `" + kafka + "`";
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DescribeTopicsAction extends KafkaAction {
        protected List<String> topics;

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static final class DescribeTopicsActionAssertions {
            protected String name;
            protected Integer partitions;
            protected Integer replicationFactor;
        }

        protected List<DescribeTopicsActionAssertions> assertions = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Describing topic" +
                    (topics.size() == 1 ? "" : "s")
                    + " " + topics
                    .stream()
                    .collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateTopicsAction extends KafkaAction {

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static final class CreateTopicRequest {
            protected String name;
            protected Integer partitions = 1;
            protected Integer replicationFactor = 1;
            protected LinkedHashMap<String, String> config = new LinkedHashMap<>();
        }

        protected LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        protected List<CreateTopicRequest> topics;
        protected Boolean assertError = false;
        protected List<String> assertErrorMessages = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating topic" +
                    (topics.size() == 1 ? "" : "s")
                    + " " + topics
                    .stream()
                    .map(CreateTopicRequest::getName)
                    .collect(joining(",", "`", "`"));
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            String markdown = "Creating topic" +
                    (topics.size() == 1 ? "" : "s")
                    + " " + topics
                    .stream()
                    .map(CreateTopicRequest::getName)
                    .collect(joining(",", "`", "`"))
                    + " on `" + kafka + "`\n";

            for (CreateTopicRequest topic : topics) {
                markdown += "* topic `" + topic.getName() + "` with partitions:" + topic.getPartitions() + " replication-factor:" + topic.getReplicationFactor();
            }

            return markdown;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AlterTopicAction extends KafkaAction {

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static final class AlterTopicRequest {
            protected String name;
            protected LinkedHashMap<String, String> config = new LinkedHashMap<>();
        }

        public List<AlterTopicRequest> topics;
        public Boolean assertError = false;
        public List<String> assertErrorMessages = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Alter topics " + topics.stream()
                    .map(AlterTopicRequest::getName)
                    .collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ProduceAction extends KafkaAction {
        protected LinkedList<Message> messages;
        protected LinkedHashMap<String, String> properties;
        protected String topic;
        protected boolean assertError = false;
        protected List<String> assertErrorMessages = List.of();
        protected String acks;
        protected String compression;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Producing " + messages.size() +  " message" + (messages.size() == 1 ? "" : "s") + " in `" + topic + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }

            return "Producing " + messages.size() + " message" + (messages.size() == 1 ? "" : "s") + " in `" + topic + "` in cluster `" + kafka + "`";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ConsumeAction extends KafkaAction {
        protected LinkedList<RecordAssertion> assertions = new LinkedList<>();
        protected LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        public String groupId;
        protected Long timeout;
        protected Integer maxMessages;
        protected Integer assertSize;
        protected Boolean showRecords = false;
        protected Boolean showHeaders = false;
        protected String topic;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Consuming from `" + topic + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            return "Consuming from `" + topic + "` in cluster `" + kafka + "";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateVirtualClustersAction extends GatewayAction {
        protected String name;
        public String serviceAccount = "sa";

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating virtual cluster `" + name + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            return "Creating virtual cluster `" + name + "` on gateway `" + gateway + "`";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FailoverAction extends GatewayAction {
        protected String from;
        protected String to;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Failing over from `" + from + "` to `" + to + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Failing over from `" + from + "` to `" + to + "` on gateway `" + gateway + "`";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddInterceptorAction extends GatewayAction {
        protected LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors;
        protected String username;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Adding interceptor" + (interceptors.keySet().size() == 1 ? "" : "s") +
                    " " + interceptors
                    .values()
                    .stream()
                    .flatMap(e -> e.keySet().stream())
                    .collect(joining(",", "`", "`")) + " in `" + gateway + "`";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class RemoveInterceptorAction extends GatewayAction {
        protected List<String> names = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Remove interceptor" + (names.size() == 1 ? "" : "s") +
                    " " + names.stream().collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddTopicMappingAction extends GatewayAction {
        protected String topicPattern;
        protected String physicalTopicName;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating mapping from `" + topicPattern + "` to `" + physicalTopicName + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            return String.format("""
                            Let's tell the `%s` that topic matching the pattern `%s` need to be concentrated into the underlying `%s` physical topic.

                            > [!NOTE]
                            > You don’t need to create the physical topic that backs the concentrated topics, it will automatically be created when a client topic starts using the concentrated topic.
                            """,
                    getGateway(),
                    topicPattern,
                    physicalTopicName);
        }

        @Data
        @Builder
        public static class TopicMapping {
            protected String physicalTopicName;
            public boolean readOnly = false;
            public boolean concentrated = true;
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ListInterceptorAction extends GatewayAction {
        protected String vcluster;
        public Integer assertSize;
        protected List<String> assertNames = List.of();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Listing interceptors for `" + vcluster + "`";
        }

        @Override
        public String getMarkdown() {
            if (StringUtils.isNotBlank(markdown)) {
                return markdown;
            }
            return "Listing interceptors on `" + gateway + "` for virtual cluster `" + vcluster + "`";
        }
    }

    public static class DocumentationAction extends Action {
    }

    @Data
    public static class IntroductionAction extends Action {
        public int headerLevel = 1;
        protected String title = "Introduction";
    }

    @Data

    public static class AsciinemaAction extends Action {
        protected String title = "View the full demo in realtime";
        public String markdown = """
                                
                You can either follow all the steps manually, or just enjoy the recording 
                                
                [![asciicast](https://asciinema.org/a/ASCIINEMA_UID.svg)](https://asciinema.org/a/ASCIINEMA_UID)
                                
                """;
    }

    @Data
    public static class ConclusionAction extends Action {
        public int headerLevel = 1;
        protected String title = "Conclusion";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileAction extends Action {
        protected String filename;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return switch (filename) {
                case "docker-compose.yaml" -> "Review the docker compose environment";
                default -> "Review `" + filename + "`";
            };
        }
    }

    @Data
    public static class GatewayAction extends Action {
        protected String gateway;
        protected String vcluster;
    }

    public static class StepAction extends Action {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DescribeKafkaPropertiesAction extends KafkaAction {
        public List<String> assertKeys = List.of();
        public List<String> assertValues = List.of();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static abstract class CommandAction extends KafkaAction {
        public boolean showOutput = true;
        public boolean isDaemon = false;
        public Integer assertError;
        public Integer assertExitCode;
        public List<String> assertOutputContains = List.of();
        public List<String> assertOutputDoesNotContain = List.of();

        abstract public String getCommand();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShAction extends CommandAction {
        public String script;
        public int iteration = 1;

        @Override
        public String getCommand() {
            return script;
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Message {
        protected LinkedHashMap<String, String> headers;
        protected String key;
        protected String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RecordAssertion {
        protected String description;
        protected LinkedHashMap<String, Assertion> headers = new LinkedHashMap<>();
        protected List<Assertion> headerKeys = List.of();
        private Assertion key;
        private Assertion value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InterceptorAssertion {
        protected String name;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Assertion {
        protected String operator = "isEqualTo";
        protected String expected;
    }

    public enum ActionType {
        STEP,
        INTRODUCTION,
        CONCLUSION,
        FILE,
        CREATE_TOPICS,
        CREATE_VIRTUAL_CLUSTERS,
        ADD_TOPIC_MAPPING,
        LIST_TOPICS,
        DESCRIBE_TOPICS,
        ALTER_TOPICS,
        PRODUCE,
        CONSUME,
        ADD_INTERCEPTORS,
        REMOVE_INTERCEPTORS,
        LIST_INTERCEPTORS,
        SH,
        DOCKER,
        DESCRIBE_KAFKA_PROPERTIES,
        FAILOVER,
        ASCIINEMA;
    }
}