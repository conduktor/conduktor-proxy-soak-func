package io.conduktor.gateway.soak.func.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.LinkedList;

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
            @JsonSubTypes.Type(value = Action.class, name = "CREATE_TOPIC")
    })
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        private ActionType type;
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
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class FetchAction extends Action {
        private LinkedList<RecordAssertion> assertions;
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
    public static class Assertion {
        private String operator = "isEqualTo";
        private Object expected;
    }

    public enum ActionType {
        CREATE_TOPIC,
        PRODUCE,
        FETCH
    }

    public enum ActionTarget {
        KAFKA,
        GATEWAY
    }
}


