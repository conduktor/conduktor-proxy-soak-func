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
    private String title;
    private Map<String, Service> services;
    private LinkedList<Action> actions;

    public Map<String, Properties> toServiceProperties() {
        Map<String, Properties> ret = new HashMap<>();
        getServices().forEach((name, s) -> ret.put(name, s.toProperties()));
        return ret;
    }

    @Override
    public String toString() {
        return title;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Service {
        private Map<String, Object> docker;
        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        private LinkedHashMap<String, String> environment = new LinkedHashMap<>();

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
        private ActionType type;
        private Integer headerLevel;
        public String title = "";
        public String markdown = "";
        public String gateway;

        public String simpleMessage() {
            return type + " " + title;
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
        public String kafka;
        public String kafkaConfig;
        public LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    }

    @Data
    public static class DockerAction extends CommandAction {
        public String command;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ListTopicsAction extends KafkaAction {
        public Integer assertSize;
        private List<String> assertExists = new ArrayList<>();
        private List<String> assertDoesNotExist = new ArrayList<>();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DescribeTopicsAction extends KafkaAction {
        public List<String> topics;

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static final class DescribeTopicsActionAssertions {
            private String name;
            private Integer partitions;
            private Integer replicationFactor;
        }

        public List<DescribeTopicsActionAssertions> assertions;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateTopicsAction extends KafkaAction {

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static final class CreateTopicRequest {
            private String name;
            private int partitions;
            private int replicationFactor;
            private LinkedHashMap<String, String> config = new LinkedHashMap<>();
        }

        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        private List<CreateTopicRequest> topics;
        private boolean assertError = false;
        private List<String> assertErrorMessages = new ArrayList<>();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating topics " + topics.stream()
                    .map(CreateTopicRequest::getName)
                    .collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ProduceAction extends KafkaAction {
        private LinkedList<Message> messages;
        private LinkedHashMap<String, String> properties;
        private String topic;
        private Boolean assertError;
        private List<String> assertErrorMessages = new ArrayList<>();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Producing " + messages.size() + (messages.size() == 1 ? "" : "s") + " messages in " + topic;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ConsumeAction extends KafkaAction {
        private LinkedList<RecordAssertion> assertions = new LinkedList<>();
        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        private String groupId;
        private Long timeout;
        private Integer maxMessages;
        private Integer assertSize;
        private boolean showRecords = false;
        private boolean showHeaders = false;
        private String topic;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Consuming from " + topic;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateVirtualClustersAction extends GatewayAction {
        public String name;
        public String serviceAccount = "sa";

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating virtual topic `" + name + "`";
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FailoverAction extends GatewayAction {
        public String from;
        public String to;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Failing over from `" + from + "` to `" + to + "`";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddInterceptorAction extends GatewayAction {
        private LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Adding interceptor" + (interceptors.keySet().size() == 1 ? "" : "s") +
                    " " + interceptors.keySet().stream().collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class RemoveInterceptorAction extends GatewayAction {
        private List<String> names = new ArrayList<>();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Removing interceptor" + (names.size() == 1 ? "" : "s") +
                    " " + names.stream().collect(joining(",", "`", "`"));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddTopicMappingAction extends GatewayAction {
        private String topicPattern;
        private String physicalTopicName;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Creating mapping `" + topicPattern + "` to `" + physicalTopicName + "`";
        }

        @Data
        @Builder
        public static class TopicMapping {
            public String physicalTopicName;
            public boolean readOnly = false;
            public boolean concentrated = true;
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ListInterceptorAction extends GatewayAction {
        public String vcluster;
        public Integer assertSize;
        private List<String> assertNames = new ArrayList<>();

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Listing interceptors on `" + gateway + "`";
        }
    }

    public static class DocumentationAction extends Action {
    }

    @Data
    public static class IntroductionAction extends Action {
        public int headerLevel = 1;
        public String title = "Introduction";
    }

    @Data

    public static class AsciinemaAction extends Action {
        public String title = "View the full demo in realtime";
        public String markdown = """
                                
                You can either follow all the steps manually, or just enjoy the recording 
                                
                [![asciicast](https://asciinema.org/a/ASCIINEMA_UID.svg)](https://asciinema.org/a/ASCIINEMA_UID)
                                
                """;
    }

    @Data
    public static class ConclusionAction extends Action {
        public int headerLevel = 1;
        public String title = "Conclusion";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileAction extends Action {
        public String filename;

        @Override
        public String getTitle() {
            if (StringUtils.isNotBlank(title)) {
                return title;
            }
            return "Review `" + filename + "`";
        }
    }

    @Data
    public static class GatewayAction extends Action {
        public String gateway;
        public String vcluster;
    }

    public static class StepAction extends Action {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DescribeKafkaPropertiesAction extends KafkaAction {
        public List<String> assertKeys = new ArrayList<>();
        public List<String> assertValues = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static abstract class CommandAction extends KafkaAction {
        public boolean showOutput = false;
        public boolean isDaemon = false;
        public Integer assertError;
        public Integer assertExitCode;
        public List<String> assertOutputContains = new ArrayList<>();
        public List<String> assertOutputDoesNotContain = new ArrayList<>();

        abstract public String getCommand();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShAction extends CommandAction {
        public String script;

        @Override
        public String getCommand() {
            return script;
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Message {
        private LinkedHashMap<String, String> headers;
        private String key;
        private String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RecordAssertion {
        private String description;
        private LinkedHashMap<String, Assertion> headers = new LinkedHashMap<>();
        private List<Assertion> headerKeys = List.of();
        private Assertion key;
        private Assertion value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InterceptorAssertion {
        private String name;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Assertion {
        private String operator = "isEqualTo";
        private String expected;
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