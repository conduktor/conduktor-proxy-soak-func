package io.conduktor.gateway.soak.func.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Scenario {
    private String title;
    private Docker docker;
    private Map<String, Properties> virtualClusters = new HashMap<>();
    private LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> plugins;
    private LinkedList<Action> actions;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Docker {
        private Service kafka;
        private Service gateway;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Service {
        private String image;
        private LinkedHashMap<String, String> environment;
        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();

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
            @JsonSubTypes.Type(value = ListInterceptorAction.class, name = "LIST_INTERCEPTORS"),
            @JsonSubTypes.Type(value = DocumentationAction.class, name = "DOCUMENTATION"),
            @JsonSubTypes.Type(value = MarkdownAction.class, name = "MARKDOWN"),
            @JsonSubTypes.Type(value = BashAction.class, name = "BASH"),
            @JsonSubTypes.Type(value = ShAction.class, name = "SH"),
            @JsonSubTypes.Type(value = StepAction.class, name = "STEP"),
            @JsonSubTypes.Type(value = DescribeKafkaPropertiesAction.class, name = "DESCRIBE_KAFKA_PROPERTIES")
    })
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        private ActionType type;
        private String description = "";

        public String simpleMessage() {
            return type + " " + description;
        }
    }

    @Data
    public static class KafkaAction extends Action {
        public String kafka;
        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();
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
            private int partitions;
            private int replicationFactor;
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
        }

        private LinkedHashMap<String, String> properties;
        private List<CreateTopicRequest> topics;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ProduceAction extends KafkaAction {
        private LinkedList<Message> messages;
        private LinkedHashMap<String, String> properties;
        private String topic;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ConsumeAction extends KafkaAction {
        private LinkedList<RecordAssertion> assertions;
        private LinkedHashMap<String, String> properties;
        private int timeout = 1000;
        private int maxMessages = 100;
        private Integer assertSize;
        private List<String> topics;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateVirtualClustersAction extends GatewayAction {
        public List<String> names;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddInterceptorAction extends GatewayAction {
        private LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class RemoveInterceptorAction extends GatewayAction {
        public String vcluster;
        private List<String> names = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ListInterceptorAction extends GatewayAction {
        public String vcluster;
        public Integer assertSize;
        private List<String> assertNames = new ArrayList<>();
    }

    public static class DocumentationAction extends Action {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MarkdownAction extends Action {
        public String markdown;
    }

    public static class GatewayAction extends Action {
        public String gateway;
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
    public static class ScriptAction extends Action {
        public String script;
        public Integer assertExitCode;
        public List<String> assertOutputContains = new ArrayList<>();
        public List<String> assertOutputDoesNotContain = new ArrayList<>();
    }


    @Data
    public static class BashAction extends ScriptAction {
    }

    @Data
    public static class ShAction extends ScriptAction {
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
        private LinkedHashMap<String, Assertion> headers;
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
        private Object expected;
    }

    public enum ActionType {
        STEP,
        DOCUMENTATION,
        MARKDOWN,
        CREATE_TOPICS,
        CREATE_VIRTUAL_CLUSTERS,
        LIST_TOPICS,
        DESCRIBE_TOPICS,
        PRODUCE,
        CONSUME,
        ADD_INTERCEPTORS,
        REMOVE_INTERCEPTORS,
        LIST_INTERCEPTORS,
        BASH,
        SH,
        DESCRIBE_KAFKA_PROPERTIES;
    }
}