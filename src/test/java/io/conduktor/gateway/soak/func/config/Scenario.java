package io.conduktor.gateway.soak.func.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Scenario {
    private String title;
    private Docker docker;
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
        private LinkedHashMap<String, String> properties;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type",
            visible = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ProduceAction.class, name = "PRODUCE"),
            @JsonSubTypes.Type(value = FetchAction.class, name = "FETCH"),
            @JsonSubTypes.Type(value = CreateTopicAction.class, name = "CREATE_TOPIC"),
            @JsonSubTypes.Type(value = AddInterceptorAction.class, name = "ADD_INTERCEPTORS"),
            @JsonSubTypes.Type(value = RemoveInterceptorAction.class, name = "REMOVE_INTERCEPTORS"),
            @JsonSubTypes.Type(value = ListInterceptorAction.class, name = "LIST_INTERCEPTORS"),
            @JsonSubTypes.Type(value = DocumentationAction.class, name = "DOCUMENTATION"),
            @JsonSubTypes.Type(value = StepAction.class, name = "STEP")
    })
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        private ActionType type;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateTopicAction extends Action {
        private ActionTarget target;
        private LinkedHashMap<String, String> properties;
        private String topic;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ProduceAction extends Action {
        private LinkedList<Message> messages;
        private ActionTarget target;
        private LinkedHashMap<String, String> properties;
        private String topic;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class FetchAction extends Action {
        private LinkedList<RecordAssertion> assertions;
        private ActionTarget target;
        private LinkedHashMap<String, String> properties;
        private String topic;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class AddInterceptorAction extends Action {
        private ActionTarget target;
        private LinkedHashMap<String, LinkedHashMap<String, PluginRequest>> interceptors;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class RemoveInterceptorAction extends Action {
        private ActionTarget target;
        public String vcluster;
        private List<String> names = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class ListInterceptorAction extends Action {
        private ActionTarget target;
        public String vcluster;
        public Integer assertSize;
        private List<String> assertNames = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentationAction extends Action {
        public String description;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StepAction extends Action {
        public String description;
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

    public static enum ActionType {
        STEP,
        DOCUMENTATION,
        CREATE_TOPIC,
        PRODUCE,
        FETCH,
        ADD_INTERCEPTORS,
        REMOVE_INTERCEPTORS,
        LIST_INTERCEPTORS;


    }

    public enum ActionTarget {
        KAFKA,
        GATEWAY
    }
}


