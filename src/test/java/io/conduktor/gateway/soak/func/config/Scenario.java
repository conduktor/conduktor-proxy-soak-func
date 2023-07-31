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
    private IO io;


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
    public static class IO {
        private Record input;
        private Record output;
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Record {
        private LinkedHashMap<String, String> headers;
        private String key;
        private String value;
    }
}


