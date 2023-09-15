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
    private Map<String, Service> services;
    private LinkedList<Action> actions;

    public Map<String, Properties> toServiceProperties() {
        Map<String, Properties> ret = new HashMap<>();
        getServices().forEach((name, s) -> ret.put(name, s.toProperties()));
        return ret;
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
            @JsonSubTypes.Type(value = ListInterceptorAction.class, name = "LIST_INTERCEPTORS"),
            @JsonSubTypes.Type(value = DocumentationAction.class, name = "DOCUMENTATION"),
            @JsonSubTypes.Type(value = MarkdownAction.class, name = "MARKDOWN"),
            @JsonSubTypes.Type(value = SuccessAction.class, name = "SUCCESS"),
            @JsonSubTypes.Type(value = ShAction.class, name = "SH"),
            @JsonSubTypes.Type(value = StepAction.class, name = "STEP"),
            @JsonSubTypes.Type(value = DescribeKafkaPropertiesAction.class, name = "DESCRIBE_KAFKA_PROPERTIES"),
            @JsonSubTypes.Type(value = FailoverAction.class, name = "FAILOVER")
    })
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        private ActionType type;
        private String title = "";
        private String markdown = "";

        public String simpleMessage() {
            return type + " " + title;
        }
    }

    @Data
    public static class KafkaAction extends Action {
        public String kafka;
        private String kafkaConfig;
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
        }

        private LinkedHashMap<String, String> properties;
        private List<CreateTopicRequest> topics;
        private Boolean assertError;
        private List<String> assertErrorMessages = new ArrayList<>();

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
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ConsumeAction extends KafkaAction {
        private LinkedList<RecordAssertion> assertions = new LinkedList<>();
        private LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        private Long timeout;
        private Integer maxMessages;
        private Integer assertSize;
        private boolean showRecords = false;
        private String topic;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateVirtualClustersAction extends GatewayAction {
        public String name;
        public String serviceAccount = "sa";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FailoverAction extends GatewayAction {
        public String from;
        public String to;
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

    public static class SuccessAction extends Action {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MarkdownAction extends Action {
        public String markdown;
    }

    @Data
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
    public static class ShAction extends KafkaAction {
        public String gateway;
        public String script;
        public boolean showOutput = false;
        public Integer assertError;
        public Integer assertExitCode;
        public List<String> assertOutputContains = new ArrayList<>();
        public List<String> assertOutputDoesNotContain = new ArrayList<>();
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
        private String expected;
    }

    public enum ActionType {
        STEP,
        SUCCESS,
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
        SH,
        DESCRIBE_KAFKA_PROPERTIES,
        FAILOVER;
    }
}