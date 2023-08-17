package io.conduktor.gateway.soak.func.config;

import lombok.AllArgsConstructor;
import lombok.Data;
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
        private String version;
        private LinkedHashMap<String, String> environment;
        private LinkedHashMap<String, String> properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Action {
        private ActionType type;
        private ActionTarget target;
        private LinkedHashMap<String, String> properties;
        private String topic;
        private LinkedList<Message> messages;
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


