package io.conduktor.gateway.soak.func.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.LinkedHashMap;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Scenario {
    private Service kafka;
    private Service gateway;
    private LinkedHashMap<String, String> plugins;
    private Workflow workflow;


    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Kafka {
        private LinkedHashMap<String, LinkedHashMap<String, String>> cases;
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Service {
        private String version;
        private LinkedHashMap<String, String> environment;
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Workflow {
        private String gatewayRequest;
        private String kafkaResponse;
        private String gatewayResponse;
    }
}


